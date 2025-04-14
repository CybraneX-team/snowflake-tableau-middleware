(function () {
    // Create connector object
    var myConnector = tableau.makeConnector();

    // Define the schema
    myConnector.getSchema = function (schemaCallback) {
        const connectionData = JSON.parse(tableau.connectionData);

        fetch('/api/schema', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: tableau.connectionData
        })
        .then(res => res.json())
        .then(schema => schemaCallback([schema]))
        .catch(err => tableau.abortWithError("Error getting schema: " + err));
    };

    // Fetch the data
    myConnector.getData = function (table, doneCallback) {
        fetch('/api/data', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: tableau.connectionData
        })
        .then(res => res.json())
        .then(data => {
            table.appendRows(data);
            doneCallback();
        })
        .catch(err => tableau.abortWithError("Error getting data: " + err));
    };

    // Register connector
    tableau.registerConnector(myConnector);
})();
