-- An example of a select query with multiple joins that returns a nested json structure.
-- This is exactly what the api would generate when GET /query/users?select=name,id,cars(make,tires(id)),motorcycles(id,name) is requested

SELECT json_group_array(json_object('name', [name], 'userId', [userId], 'cars', [cars], 'motorcycles', [motorcycles])) 
AS data 
FROM (
    SELECT [users].[name], [users].[id] AS [userId], 
    json_group_array(json_object('make', [cars].[make], 'tires', tires)) FILTER( WHERE [cars].[id] IS NOT NULL ) AS [cars],
    json_group_array(json_object('id', [motorcycles].[id], 'name', [motorcycles].[name], 'user_id', [motorcycles].[user_id])) FILTER( WHERE [motorcycles].[id] IS NOT NULL ) AS [motorcycles]
    FROM [users]
    LEFT JOIN (
        SELECT [cars].[make], [cars].[user_id], [cars].[id], json_group_array(json_object('id', [tires].[id])) FILTER( WHERE [tires].[id] IS NOT NULL ) AS [tires] FROM [cars]
        LEFT JOIN (
            SELECT [tires].[id], [tires].[car_id]
            FROM [tires]
        ) AS [tires] on [cars].[id] = [tires].[car_id]
    ) AS [cars] on [users].[id] = [cars].[user_id] 
    LEFT JOIN (
        SELECT [motorcycles].[id], [motorcycles].[name], [motorcycles].[user_id]
        FROM [motorcycles]
    ) AS [motorcycles] on [users].[id] = [cars].[user_id]
    GROUP BY [users].[id]
);

