# vancap
vancap is a small go application that will deploy a collector into a running
skupper kubernetes site to capture vanflow state and dump it to a file.

## Usage

1. Check that your current kube context points to a running skupper kube site.
    `skupper status`
1. Run vancap.
    `vancap capture.json`
1. Start the docker-compose project.
    `podman-compose up -d`
1. Post the capture file to the `fixture` container.
    `curl -X POST -d @capture.json localhost:9080`
1. Browse to the console running at `http://localhost:8010`. It should begin to update with state from the capture file.

