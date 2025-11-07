# SAS Customer Intelligence 360 REST API Collections

[![Insomnia](https://img.shields.io/badge/Insomnia-Compatible-purple.svg)](https://insomnia.rest/)

A comprehensive collection of REST API endpoints for **SAS Customer Intelligence 360** (CI 360), designed to make learning and testing easy for developers and system administrators.

## 🎯 Purpose

This repository provides a curated collection of the most commonly used CI 360 REST APIs, enabling developers to:
- Quickly test and validate API functionality
- Accelerate integration development
- Maintain consistent API testing practices
- Share standardized API collections across teams

> Important: This collection only covers commonly used APIs, there are more APIs in SAS CI 360 that you can leverage which is not mentioned here. For comprehensive API documentation, visit:
[SAS Customer Intelligence 360 REST APIs Documentation](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintapis/ch-rest-apis.htm)

## 🔧 Prerequisites

- [Insomnia REST Client](https://insomnia.rest/) (local installation required)
- Valid SAS CI 360 tenant access
- API credentials (Client ID, Client Secret, API User credentials)

## 🚨 Security Guidelines

> **⚠️ CRITICAL SECURITY NOTICE**
> 
> - **DO NOT** use cloud synchronization features in Insomnia or any other REST client
> - **DO NOT** commit files containing API credentials, secrets, or tenant URLs to version control
> - **ALWAYS** use these collections on local machines only
> - **NEVER** share collections with embedded credentials

## 📋 Installation & Setup

### 1. Import the Collection

1. Download the `Insomnia-Collection.yaml` file
2. Open Insomnia REST Client
3. Navigate to **Application** → **Import/Export** → **Import Data**
4. Select the downloaded YAML file
5. Import into your local workspace

### 2. Configure Environment Variables

After importing, update the collection's environment with your tenant-specific values:

```json
{
	"base_url": "https://ENTER YOUR TENANT'S EXTERNAL API GATEWAY URL",
	"tenant-id": "ENTER YOUR TENANT ID",
	"client-secret": "ENTER YOUR CLIENT SECRET HERE",
	"application-id": "ENTER YOUR APPLICATION ID HERE",
	"api-user": "ENTER YOU API USER NAME FROM CI360",
	"api-secret": "ENTER THE SECRET FOR THE SAME API USER",
	"copy-item-destination-JWT": "ENTER THE STATIC JWT OR ACCESS TOKEN FOR DESTINATION TENANT",
	"internal-temp-jwt": "This is automatically added.",
	"JWT": "This is automatically generated.",
	"content_path": "content",
	"full_endpoint": "{{ _.base_url }}{{ _.content_path }}/{{ _['tenant-id'] }}/",
	"id_type": "ENTER THE IDENTITY TYPE HERE [customer_id, login_id, subject_id]",
	"id_value": "ENTER THE IDENTITY VALUE HERE",
	"spot_id": "ENTER THE SPOT ID HERE",
	"spot_key": "ENTER THE SPOT KEY HERE",
	"spot_attr": "ENTER THE SPOT ATTRIBUTE HERE [ FOR EXAMPLE, topic=iphone]",
	"event_path": "events",
	"channel_type": "web",
	"visitor_id": ""
}
```

> **Note**: The `internal-temp-jwt` and `JWT` fields are automatically populated by pre-request scripts.

## 📚 API Coverage

This collection includes endpoints for the following CI 360 modules:

### 🐳 Container Image Management
- **List Images**: Retrieve all container images published from SAS CI 360
- **Container Token Generation**: Generate authentication tokens required for pulling images from the container registry

### 🛡️ Marketing Administration
- **Audit Records**: Track system activities and changes
- **Token Management**: Handle authentication tokens

### 👥 Marketing Audience
- Audience management and segmentation APIs

### 📊 Marketing Data
- **Customer Tables**: Manage customer data structures
- **GDPR Requests**: Handle data privacy compliance
- **Customer Identities**: Manage customer identity resolution
- **Identity Records**: Process identity data

### 🚀 Marketing Execution
- **Segment Map Jobs**: Execute segmentation workflows
- **Bulk Task Execution**: Manage large-scale task processing
- **Occurrences**: Handle event occurrences

### 🎨 Marketing Design
- **Tasks**: Manage marketing tasks and campaigns

### 🌐 Marketing Gateway
- **External Events**: Process single and batch external events
- **Agent Downloads**: Download various CI 360 agents
- **Access Point Configurations**: Manage access point settings

### 🖥️ Server-Side APIs *(Coming Soon)*
- **Server-Side Personalization**: Enable personalization without client-side JavaScript
- **Data Collection**: Collect customer data server-side for channels that cannot implement CI 360 JavaScript Tag or Mobile SDK
- **Event Tracking**: Server-side event processing and analytics

### 📋 Additional APIs
- **Copy Item**: Replicate items between tenants
- **SCIM APIs**: User and group management

## 🔗 Documentation

For comprehensive API documentation, visit:
[SAS Customer Intelligence 360 REST APIs Documentation](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintapis/ch-rest-apis.htm)

## 🚀 Quick Start Guide

1. **Import Collection**: Load the `Insomnia-Collection.yaml` into Insomnia
2. **Configure Environment**: Update all environment variables with your tenant details
3. **Test Connection**: Start with basic APIs like "List All Tasks" to verify connectivity
4. **Explore APIs**: Navigate through the organized folder structure to find relevant endpoints


## 🔧 Troubleshooting

### Common Issues

**Authentication Failures**
- Verify `client-secret` and `application-id` are correct
- Ensure API user has proper permissions
- Check if JWT tokens have expired or not
- Check if your Access Point in CI 360 is active or not.

**API Endpoint Errors**
- Confirm `base_url` matches your tenant's gateway
- Verify `tenant-id` is correctly formatted

## 🆘 Support & Resources

- 📖 [Official SAS CI 360 Documentation](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintapis/ch-rest-apis.htm)
- 🎫 Contact SAS Support for tenant-specific or API-related issues

---

**Disclaimer**: This is an unofficial collection created for developer convenience. Always refer to official SAS documentation for the most up-to-date API specifications and best practices.

The content in legacy folder will not be maintained. 



