#!/bin/bash
echo "Starting Azure Login..."
az login --service-principal --username $APPID --password $APPPW --tenant $TENANT --output table
az account set --subscription $SUBSCRIPTION --output table
isLogined=$?
if [ $isLogined != 0 ]
then
    echo "================================================================================"
    printf '%40s\n' " Error Occured, Require admin check. "
    echo "================================================================================"
    HEADER="Content-Type: application/json"
    DATA=$(cat <<EOF
        {
            "type":"message",
            "attachments":[
                {
                    "contentType":"application/vnd.microsoft.card.adaptive",
                    "contentUrl":null,
                    "content":{
                        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
                        "type":"AdaptiveCard",
                        "version":"1.2",
                        "body":[
                            {
                            "type": "TextBlock",
                            "text": "[FAILED] Login to Azure Tanent failed",
                            "wrap": true,
                            "size": "Large",
                            "weight": "Bolder",
                            "color": "Attention"
                            },
                            {
                                "type": "TextBlock",
                                "text": "Please check the service principal credentials and permissions.",
                                "wrap": true
                            }
                        ]
                    }
                }
            ]
        }
EOF
    )
    echo "WEBHOOK_URL : " $WEBHOOK_URL
    curl -X POST $WEBHOOK_URL -H "$HEADER" -d "$DATA" -w "\nHTTP Status: %{http_code}\n"
    exit 1;
fi
echo "--------------------------------------------------------------------------------"
printf '%40s\n' "Login to Azure Tanent successfully."
echo "--------------------------------------------------------------------------------"

echo "Starting VM..."
az vm start --name $VM_NAME --no-wait --resource-group $RESOURCE_GROUP --output table
isStartedVM=$?
if [ $isStartedVM != 0 ]
then
    echo "================================================================================"
    printf '%40s\n' " Error Occured, Require admin check. "
    echo "================================================================================"
    HEADER="Content-Type: application/json"
    DATA=$(cat <<EOF
        {
            "type":"message",
            "attachments":[
                {
                    "contentType":"application/vnd.microsoft.card.adaptive",
                    "contentUrl":null,
                    "content":{
                        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
                        "type":"AdaptiveCard",
                        "version":"1.2",
                        "body":[
                            {
                            "type": "TextBlock",
                            "text": "[FAILED] Start VM failed",
                            "wrap": true,
                            "size": "Large",
                            "weight": "Bolder",
                            "color": "Attention"
                            },
                            {
                                "type": "TextBlock",
                                "text": "Please check the VM name and resource group.",
                                "wrap": true
                            }
                        ]
                    }
                }
            ]
        }
EOF
    )
    echo "WEBHOOK_URL : " $WEBHOOK_URL
    curl -X POST $WEBHOOK_URL -H "$HEADER" -d "$DATA" -w "\nHTTP Status: %{http_code}\n"
    exit 1;
fi

echo "--------------------------------------------------------------------------------"
printf '%40s\n' "VM start command sent successfully."
echo "--------------------------------------------------------------------------------"