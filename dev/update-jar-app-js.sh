shadow release app

# jar tf target/crux-console.jar | grep main.js
# zip -d target/crux-console.jar static/crux-ui/compiled/main.js

jar uf target/crux-console.jar -C resources static/crux-ui/compiled/main.js
# says:
# jar update file name.jar -Cd to resources and put path/main.js into it
