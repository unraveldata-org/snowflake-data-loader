CREATE OR REPLACE PROCEDURE DELETE_TASKS(DB_NAME STRING, SCHEMA_NAME STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
function deleteTasks() {
    var stmt = snowflake.createStatement({
        sqlText: `SHOW TASKS IN SCHEMA ${DB_NAME}.${SCHEMA_NAME}`
    });

    var resultSet = stmt.execute();
    var tasksDeleted = 0;
    var deletedTasks = [];

    while (resultSet.next()) {
        var taskName = resultSet.getColumnValue("name");
        var dbName   = resultSet.getColumnValue("database_name");
        var schemaNm = resultSet.getColumnValue("schema_name");

        var fullTaskName = `${dbName}.${schemaNm}.${taskName}`;

        var dropStmt = snowflake.createStatement({
            sqlText: `DROP TASK IF EXISTS ${fullTaskName}`
        });
        dropStmt.execute();

        deletedTasks.push(fullTaskName);
        tasksDeleted++;
    }

    if (tasksDeleted === 0) {
        return `No tasks found in "${DB_NAME}.${SCHEMA_NAME}".`;
    } else {
        return `Deleted ${tasksDeleted} task(s): ` + deletedTasks.join(', ');
    }
}

try {
    return deleteTasks();
} catch (err) {
    return 'Error: ' + err.message;
}
$$;



-- Execute the procedure to delete tasks for a specific database and schema
-- Replace <dbName> and <schemaName> with your actual database and schema names
CALL DELETE_TASKS('<dbName>','<schemaName>');