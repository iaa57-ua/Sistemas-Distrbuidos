************************* EC_REQUEST.JSON 14/10/24 *********************************
The "EC_Request.json" file MUST be used to specify the services requested for each customer. Each customer will have their own EC_Request.json file, which will naturally vary between customers. 

When the customer application runs, it will read this file and send the first service request to the central system. Once the first request is completed, the application will continue sending the next request until all requests in the file have been processed.
