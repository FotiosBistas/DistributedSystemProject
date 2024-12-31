# DistributedSystemProject
Project for the first semester course in Distributed System 


# Upload the function to Azure Function app 
Each directory is a seperate function app. The preprocessing directory segments the video and uploads them into the `input-segments-container`.

Test if function app works locally:

```
func start
```

Publish the function app to the cloud: 

```
func azure functionapp publish video-preprocessing --python --force
```
Force rebuilds the app.

You should see it in the overview page of the function app: 

!["Function App Overview"](./images/function_app_overview.png "Function App Overview")


Clicking on the function you are able to see it's code, it's logs and debug it: 

![](./images/function_app_details.png "Function App Details")


You can set environment variables here: 

![](./images/environment_variables.png "Environment Variables")

