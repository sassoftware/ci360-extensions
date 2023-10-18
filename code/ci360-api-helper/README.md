# CI360 API Helper

The Customer Intelligence 360 API helper is a utility which can be used to work with the SAS Customer Intelligence 360 APIs. It provides a user interface to invoke API's for sending events and maintaining tables. These API's help to deliver a seamlessly integrated user experience from one device to the next. 
On the home page of the API helper you can see a drop-down list showing a list of pre saved configurations. By selecting any tenant you want to login, all the other details such as API Gateway, Tenant ID and secret will be automatically populated in the below fields.

## Prerequisites
It is advisable that you have a basic understanding of the following topics before using the utility:

* Basic knowledge of CI360 APIs.
* Understanding Linux as well as Docker commands
* Make sure docker is installed on the machine you want to run this utility on. 

#### Make sure you have the following information at hand:
* Tenant Name, API Gateway, Tenant ID, Tenant Secret, CI360 URL, API User, API secret. (These all details can be found in CI360 UI – General Settings – Access Point – click on your Access Point)

#### Docker Environment
Make sure you have the ability to:
* Create a docker container based on image available
* Run container


## Installation

To download the code from a GitHub repository and create a Docker container based on an image available in the project's root directory, you can follow these steps:

Clone the GitHub repository to your local machine using the git clone command. Replace <repository-url> with the actual URL of the repository:

```shell
git clone <repository-url>
```

Change to the project's directory:

```shell
cd <repository-directory>
```

Use the docker build command to build the Docker image. Replace <image-name> with the desired name for your image and . with the current directory (assuming the Dockerfile is located in the project's root directory):

```shell
docker build -t <image-name> .
```
Run the Docker container using the docker run command. Replace <container-name> with the desired name for your container and <image-name> with the name of the Docker image you built in the previous step:

```shell
docker run --name <container-name> <image name>
```

Now you have API helper running on port 80 of your server.
