package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

const (
	pollPeriod = 1 * time.Second
)

var (
	log            = logf.Log.WithName("peer-list")
	onChange       = flag.String("on-change", "", "Script to run on change, must accept a new line separated list of peers via stdin.")
	onStart        = flag.String("on-start", "", "Script to run on start, must accept a new line separated list of peers via stdin.")
	allowed_nets   = flag.String("allowed-nets", "2000::/3", "Allowed IP nets")
	pid_name       = flag.String("pid-name", "yandex.net/net-prjid", "Annotation name with project id")
	netstatus_name = flag.String("annotation-name", "k8s.v1.cni.cncf.io/network-status", "Annotation name with network status")
	namespace      = flag.String("ns", "", "The namespace this pod is running in. If unspecified, the POD_NAMESPACE env var is used.")
	svc            = flag.String("service", "", "Governing service responsible for the DNS records of the domain this pod is in.")
)

type NetDesc struct {
	Ips []string
}

func parseNets(nets []string) ([]*net.IPNet, error) {
	parsedNets := make([]*net.IPNet, len(nets))
	for i, subnet := range nets {
		_, ipNet, err := net.ParseCIDR(subnet)
		if err != nil {
			return parsedNets, fmt.Errorf("cannot parse net %s: %s", subnet, err)
		}
		parsedNets[i] = ipNet
	}
	return parsedNets, nil
}

func isMatchAnyNet(ipAddr net.IP, nets []*net.IPNet) bool {
	for _, ipNet := range nets {
		if ipNet.Contains(ipAddr) {
			return true
		}
	}
	return false
}

// IsIPFromProjectID returns true is IP is from particular projectID
func IsIPFromProjectID(ip net.IP, projectID string) (bool, error) {
	id, err := strconv.ParseUint(projectID, 16, 32)
	if err != nil {
		return false, fmt.Errorf("invalid project id: %w", err)
	}

	match := true
	const idPos = 8 /* 64..95 bits */
	const idBytes = 4

	for i := uint8(0); i < idBytes; i++ {
		if byte(id>>(idPos*(idBytes-i-1))) != ip[i+idPos] {
			match = false
		}
	}
	return match, nil
}

func lookup_cni(netStatus string, allowedNets []*net.IPNet, allowedPids []string) (net.IP, error) {

	var jdata []NetDesc
	err := json.Unmarshal([]byte(netStatus), &jdata)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal network status: %w", err)
	}

	for _, v := range jdata {
		//fmt.Printf("val: %v: %v\n", k, v)

		for _, ip := range v.Ips {
			ipAddr := net.ParseIP(ip)
			//fmt.Printf(" ipaddr: %w\n", ipAddr)
			if isMatchAnyNet(ipAddr, allowedNets) && len(ipAddr) == net.IPv6len {
				for _, pid := range allowedPids {
					//fmt.Printf("  * %s %s\n", ipAddr, pid)

					match, err := IsIPFromProjectID(ipAddr, pid)
					if err != nil {
						return nil, fmt.Errorf("IsIPFromProjectID(%s, %s) failed: %w", ipAddr, pid, err)
					}
					if match {
						//fmt.Printf("    matched pid %v\n", pid)
						return ipAddr, nil
					}
				}
			}
		}
	}

	return nil, fmt.Errorf("IP addr not found")
}

func shellOut(sendStdin, script string) {
	log.Info("executing", "script", script, "stdin", sendStdin)
	// TODO: Switch to sending stdin from go
	out, err := exec.Command("bash", "-c", fmt.Sprintf("echo -e '%v' | %v", sendStdin, script)).CombinedOutput()
	if err != nil {
		log.Error(err, "failed to execute", "script", script, "out", string(out))
	}
	log.Info("execution result", "script", script, "out", string(out))
}

func main() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGUSR1)
	go func() {
		<-signalChan
		os.Exit(0)
	}()

	flag.Parse()

	opts := zap.Options{Development: true}
	logf.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	allowedSubNets := strings.Split(*allowed_nets, ",")

	ns := *namespace
	if ns == "" {
		ns = os.Getenv("POD_NAMESPACE")
	}
	if ns == "" {
		ns_file := "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
		content, err := os.ReadFile(ns_file)
		if err != nil {
			log.Error(err, "cannot read namespace from file", "file", ns_file)
		} else {
			ns = string(content)
		}
	}
	log.Info("Peer finder enter", "allowedSubNets", allowedSubNets, "namespace", ns)
	allowedNets, err := parseNets(allowedSubNets[:])
	if err != nil {
		panic(err)
	}

	if *onChange == "" && *onStart == "" {
		panic(fmt.Errorf("Incomplete args, require -on-change and/or -on-start."))
	}

	script := *onStart
	if script == "" {
		script = *onChange
		log.Info("no on-start supplied, on-change will be applied on start.", "on-change", script)
	}

	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	for peers := sets.NewString(); script != ""; time.Sleep(pollPeriod) {
		pods, err := clientset.CoreV1().Pods(ns).List(context.TODO(), metav1.ListOptions{
			LabelSelector: "app.kubernetes.io/component=mysql",
		})
		if err != nil {
			panic(err.Error())
		}
		log.Info("pods in the cluster", "count", len(pods.Items))

		newPeers := sets.NewString()
		for _, pod := range pods.Items {
			//fmt.Printf("## pod %s: %s\n", pod.Name, pod.Annotations)

			var allowedPids []string
			if v, ok := pod.Annotations[*pid_name]; ok {
				allowedPids = strings.Split(v, ",")
				log.Info("annotation", "allowedPids", allowedPids)
			} else {
				log.Error(fmt.Errorf("no annotation with Project ID"), "pod", pod.Name, "pid_name", pid_name, "annotation", v)
				continue
			}

			if v, ok := pod.Annotations[*netstatus_name]; ok {
				//log.Info("annotation", "network-status", v)
				peer, err := lookup_cni(v, allowedNets, allowedPids)
				if err != nil {
					continue
				}
				log.Info("lookup", "IP", peer)
				newPeers.Insert(peer.String())
			} else {
				log.Error(fmt.Errorf("no annotation with networt-status"), "pod", pod.Name, "netstatus_name", netstatus_name)
				continue
			}
		}

		log.Info("lookup", "newPeers", newPeers)
		peerList := newPeers.List()
		sort.Strings(peerList)
		if strings.Join(peers.List(), ":") != strings.Join(newPeers.List(), ":") {
			log.Info("Peer list updated", "old", peers.List(), "new", newPeers.List())
			shellOut(strings.Join(peerList, "\n"), script)
			peers = newPeers
		}
		script = *onChange
	}
	// TODO: Exit if there's no on-change?
	log.Info("Peer finder exiting")
}
