# dos-dss-lambda

This presents an [Amazon Lambda](https://aws.amazon.com/lambda/) microservice
following the [Data Object Service](https://github.com/ga4gh/data-object-service-schemas) ([view the OpenAPI description](https://ga4gh.github.io/data-object-service-schemas/)!).
It allows data in the [Human Cell Atlas Data Store](https://github.com/HumanCellAtlas/data-store)
to be accessed using Data Object Service APIs.

## Using the service

A development version of this service is available at https://ekivlnizh1.execute-api.us-west-2.amazonaws.com/api/ .
To make proper use of the service, one can either use cURL or an HTTP client to write API requests
following the [OpenAPI description](https://spbnq0bc10.execute-api.us-west-2.amazonaws.com/api/swagger.json).

```
# Will request the first page of Data Bundles from the service.
curl https://ekivlnizh1.execute-api.us-west-2.amazonaws.com/api/ga4gh/dos/v1/databundles
```

There is also a Python client available, that makes it easier to use the service from code.

```
from ga4gh.dos.client import Client
client = Client("https://ekivlnizh1.execute-api.us-west-2.amazonaws.com/api/")
local_client = client.client
models = client.models
local_client.ListDataBundles().result()
```

For more information refer to the [Data Object Service](https://github.com/ga4gh/data-object-service-schemas).

### Authentication

If you're using a DSS endpoint that requires authentication, you'll need to
include Google Cloud Platform credentials in the form of a `gcp-credentials.json`
file. You can create that file by following steps 2 and 3
[here](https://github.com/HumanCellAtlas/data-store#gcp).

Once you have your credential file, move it to the `chalicelib/` directory so
that it's included in the Lambda deployment package, then specify the file to
dos-dss-lambda by setting the `GOOGLE_APPLICATION_CREDENTIALS` environment
variable to the JSON file path (i.e. `chalicelib/gcp-credentials.json`) in
`.chalice/config.json`.

## Development

### Status

This software is being actively developed to provide the greatest level of feature parity
between DOS and DSS. It also presents an area to explore features that might extend the DOS
API. Current development items can be seen in [the Issues](https://github.com/DataBiosphere/dos-dss-lambda/issues).

### Feature development

The Data Object Service can present many of the features of the DSS API naturally. This
lambda should present a useful client for the latest releases of the DSS API.

In addition, the DOS schemas may be extended to present available from the DSS, but
not from DOS.

#### DSS Features

* Subscriptions
* Authentication
* Querying
* Storage management

#### DOS Features

* File listing
  *  The DSS API presents bundle oriented indices and so listing all the details of files
     can be a challenge.
* Filter by URL
  *  Retrieve bundle entries by their URL to satisfy the DOS List request.

#### Creating BDBags using the `dss-dos-lambda`
Using a list of DSS Data Bundles you can create a remote-file-manifest (RFM). That
RFM is then used to create a [BDBag](https://github.com/fair-research/bdbag/blob/master/doc/config.md)
(see `create_bdbag` Jupyter notebook).

### Installing and Deploying

The gateway portion of the AWS Lambda microservice is provided by chalice. So to manage
deployment and to develop you'll need to install chalice.

Once you have installed chalice, you can download and deploy your own version of the
service.

```
pip install chalice
git clone https://github.com/DataBiosphere/dos-dss-lambda.git
cd dos-dss-lambda
chalice deploy
```

Chalice will return a HTTP location that you can issue DOS requests to. You can then use
HTTP requests in the style of the [Data Object Service](https://ga4gh.github.io/data-object-service-schemas).

### Accessing data using DOS client

A Python client for the Data Object Service is made available [here](https://github.com/ga4gh/data-object-service-schemas/blob/master/python/ga4gh/dos/client.py).
Install this client and then view the example in [Example Usage](https://github.com/DataBiosphere/dos-dss-lambda/blob/master/example-usage.ipynb).
This notebook will guide you through basic read access to data in the DSS via DOS.

### Issues

If you have a problem accessing the service or deploying it for yourself, please head
over to [the Issues](https://github.com/DataBiosphere/dos-dss-lambda/issues) to let us know!

### Release strategy

Releases are marked with a GitHub Release and a tagged commit in the format `x.y.z`. At the time of writing, this project
is not being continuously deployed.

## TODO

* Validation
* Error handling
* Aliases
* Filter by URL

```
+------------------+      +--------------+        +--------+
| ga4gh-dos-client |------|dos-dss-lambda|--------|DSS API |
+--------|---------+      +--------------+        +--------+
         |                        |
         |                        |
         |------------------swagger.json
```

We have created a lambda that creates a lightweight layer that can be used
to access data in the HCA DSS using GA4GH libraries.

The lambda accepts DOS requests and converts them into requests against
DSS endpoints. The results are then translated into DOS style messages before
being returned to the client.

To make it easy for developers to create clients against this API, the Open API
description is made available.


