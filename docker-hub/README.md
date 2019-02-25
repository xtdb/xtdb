Pull the Docker image:
`docker pull refset/standalone-demo-with-volume:0_0`
Run:
`mkdir my-crux-data & docker run -p 8079:8080 -v $(pwd)/my-crux-data:/usr/src/app/data -i -t juxt/standalone-demo-with-volume:0_0 sh -c "lein run"`
Navigate to: `http://localhost:8079`
