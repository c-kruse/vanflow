package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

var (
	flags *flag.FlagSet = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	Duration string
	Tag      string
	Debug    bool

	home = os.Getenv("HOME")
)

func main() {
	flags.Usage = func() {
		fmt.Printf(`Usage: %s [options...] <output file>

A tool to caputre vanflow state.
`, os.Args[0])
		flags.PrintDefaults()
	}
	flags.StringVar(&Duration, "duration", "10s", "time to wait for vanflow capture in go time.Duration string format")
	flags.StringVar(&Tag, "tag", "latest", "vanflow-tool image tag")
	flags.BoolVar(&Debug, "debug", false, "enable debug logging")
	flags.Parse(os.Args[1:])
	if len(flags.Args()) != 1 {
		fmt.Printf("error: expected single argument for output file. got %v\n", flags.Args())
		flags.Usage()
		os.Exit(1)
	}

	duration, err := time.ParseDuration(Duration)
	if err != nil {
		fmt.Printf("error parsing duration: %s\n", err)
		flags.Usage()
		os.Exit(1)
	}
	if Debug {
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
		slog.SetDefault(logger)
	}

	namespace, restcfg, clientset, err := setupKube()
	if err != nil {
		fmt.Printf("failed to get kube client %q: %s\n", namespace, err)
		flags.Usage()
		os.Exit(1)
	}

	ctx := context.Background()
	deploymentsClient := clientset.AppsV1().Deployments(namespace)
	podsClient := clientset.CoreV1().Pods(namespace)
	_, err = deploymentsClient.Get(ctx, "skupper-router", metav1.GetOptions{})
	if err != nil {
		fmt.Printf("skupper not installed in namespace %q: %s\n", namespace, err)
		flags.Usage()
		os.Exit(1)
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "vanflow-server",
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": "vanflow-server",
				},
			},
			Template: apiv1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": "vanflow-server",
					},
				},
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Name:            "vanflow-server",
							Image:           fmt.Sprintf("quay.io/ckruse/vanflow-tool:%s", Tag),
							ImagePullPolicy: apiv1.PullAlways,
							SecurityContext: &apiv1.SecurityContext{
								RunAsNonRoot: ptr(true),
							},
							Args: []string{
								"-messaging-config=/etc/messaging/connect.json",
								"serve",
							},
							VolumeMounts: []apiv1.VolumeMount{
								{
									MountPath: "/etc/messaging/",
									Name:      "skupper-local-client",
								},
							},
						},
					},
					Volumes: []apiv1.Volume{
						{
							Name: "skupper-local-client",
							VolumeSource: apiv1.VolumeSource{
								Secret: &apiv1.SecretVolumeSource{
									SecretName:  "skupper-local-client",
									DefaultMode: ptr(int32(420)),
								},
							},
						},
					},
				},
			},
		},
	}

	// Create Deployment
	slog.Debug("creating deployment")
	_, err = deploymentsClient.Create(ctx, deployment, metav1.CreateOptions{})
	if err != nil {
		panic(err)
	}

	defer func() {
		cleanupCtx, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()
		slog.Debug("cleaning up deployment", slog.String("name", deployment.Name))
		if err := deploymentsClient.Delete(cleanupCtx, deployment.Name, metav1.DeleteOptions{}); err != nil {
			slog.Error("error cleaning up deployment", slog.String("name", deployment.Name), slog.Any("error", err))
		}
	}()

	b := backoff.WithContext(backoff.NewExponentialBackOff(), ctx)
	var dep *appsv1.Deployment
	backoff.RetryNotify(func() error {
		dep, err = deploymentsClient.Get(ctx, deployment.Name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("deployment not found: %v", err)
		}
		if dep.Status.ReadyReplicas < 1 {
			return fmt.Errorf("not ready...")
		}
		return nil
	}, b, func(err error, d time.Duration) {
		slog.Debug("deployment not ready", slog.String("delay", d.String()), slog.Any("error", err))
	})
	slog.Debug("created deployment", slog.String("name", deployment.Name))

	pods, err := podsClient.List(ctx, metav1.ListOptions{
		LabelSelector: "app=vanflow-server",
	})
	if err != nil {
		slog.Error("failed to find deployment pod", slog.Any("error", err))
		os.Exit(1)
	}
	first := pods.Items[0].Name

	roundTripper, upgrader, err := spdy.RoundTripperFor(restcfg)
	if err != nil {
		slog.Error("failed to create round tripper", slog.Any("error", err))
		os.Exit(1)
	}

	path := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/portforward", namespace, first)
	hostIP := strings.TrimLeft(restcfg.Host, "htps:/")
	serverURL := url.URL{Scheme: "https", Path: path, Host: hostIP}

	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: roundTripper}, http.MethodPost, &serverURL)
	stopChan, readyChan := make(chan struct{}, 1), make(chan struct{}, 1)
	defer close(stopChan)
	forwarder, err := portforward.New(dialer, []string{"9090"}, stopChan, readyChan, os.Stdout, os.Stderr)
	if err != nil {
		slog.Error("failed to create port forwarder", slog.Any("error", err))
		os.Exit(1)
	}
	forwardErr := make(chan error, 1)
	go func() {
		slog.Debug("starting port forward to capture vanflow state")
		if err = forwarder.ForwardPorts(); err != nil {
			forwardErr <- err
		}
	}()

	slog.Debug("waiting for vanflow state to accumulate", slog.String("delay", duration.String()))
	timer := time.NewTimer(duration)
	var ready bool
READY:
	for {
		select {
		case <-readyChan:
			ready = true
		case <-timer.C:
			break READY
		}
	}
	if !ready {
		if duration < (5 * time.Second) {
			select {
			case <-readyChan:
				ready = true
			case <-time.After(5*time.Second - duration):
			}
		}
		if !ready {
			slog.Error("failed to start port forwarder", slog.Any("error", err))
			os.Exit(1)
		}
	}

	output := flags.Arg(0)
	f, err := os.Create(output)
	if err != nil {
		slog.Error("failed to create output file", slog.Any("error", err), slog.String("name", output))
		os.Exit(1)
	}
	defer f.Close()
	slog.Debug("requesting vanflow capture")
	resp, err := http.Get("http://127.0.0.1:9090")
	if err != nil {
		slog.Error("error requesting vanflow capture", slog.Any("error", err), slog.String("name", output))
		os.Exit(1)
	}
	defer resp.Body.Close()

	slog.Debug("writing caputre file", slog.String("name", output))
	if _, err := io.Copy(f, resp.Body); err != nil {
		slog.Error("failed to write caputre file", slog.String("name", output), slog.Any("error", err))
	}

}

func setupKube() (string, *rest.Config, kubernetes.Interface, error) {
	kubeConfig, err := clientcmd.NewDefaultClientConfigLoadingRules().Load()
	if err != nil {
		return "", nil, nil, err
	}
	config := clientcmd.NewDefaultClientConfig(*kubeConfig, &clientcmd.ConfigOverrides{})
	namespace, _, err := config.Namespace()
	if err != nil {
		return namespace, nil, nil, err
	}
	restcfg, err := config.ClientConfig()
	if err != nil {
		return namespace, nil, nil, err
	}

	clientset, err := kubernetes.NewForConfig(restcfg)
	return namespace, restcfg, clientset, err
}

func ptr[T any](c T) *T { return &c }
