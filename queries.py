query_1 = 'Alter Table Soft.Clients delete where "ID Client" is not null'

query_2 = """INSERT INTO Soft.Clients
                SELECT 
                JSONExtractString(companies._airbyte_data, 'Id') AS "ID Client",
                JSONExtractString(companies._airbyte_data, 'Name') AS "Клиент Atlas",
                JSONExtractString(companies._airbyte_data, 'Inn') AS "ИНН Клиента Atlas",
                JSONExtractString(companies._airbyte_data, 'Town') AS "Регион клиента",
                JSONExtractString(companies._airbyte_data, 'StorageAddress') AS "Адрес клиента",
                concat('[', 
                        JSONExtractString(companies._airbyte_data, 'StorageLatitude'), ',',
                        JSONExtractString(companies._airbyte_data, 'StorageLongitude'), ']')   AS "Координаты клиента",
                JSONExtractString(companies._airbyte_data, 'Phone') AS "Телефон",
                JSONExtractString(companies._airbyte_data, 'Email') AS "Почта",
                if(startsWith(JSONExtractString(companies._airbyte_data, 'Additional'), '{')=1, '', JSONExtractString(companies._airbyte_data, 'Additional'))
                AS "Агенство"
            FROM Soft._airbyte_raw_cloudCompanies AS companies 
            WHERE parseDateTime32BestEffortOrNull(JSONExtractString(companies._airbyte_data, 'DeletedOn')) IS NULL"""

query_3 = "Alter Table Soft.AtlasOrder delete where 'ID Order' is not null"

query_4 = """INSERT INTO Soft.AtlasOrder 
                SELECT concat(substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(orders._airbyte_data, '_id'))), 16)), 7, 2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(orders._airbyte_data, '_id'))),16)),5,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(orders._airbyte_data, '_id'))),16)),3,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(orders._airbyte_data, '_id'))),16)),1,2),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(orders._airbyte_data, '_id'))),16)),12,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(orders._airbyte_data, '_id'))),16)),10,2),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(orders._airbyte_data, '_id'))),16)),17,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(orders._airbyte_data, '_id'))),16)),15,2),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(orders._airbyte_data, '_id'))),16)),20,4),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(orders._airbyte_data, '_id'))),16)),25,12))
                     AS "ID Order",
                     concat(substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'CompanyInfo' ), 'CompanyId')), 16)), 7, 2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'CompanyInfo'), 'CompanyId')),16)),5,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'CompanyInfo'), 'CompanyId')),16)),3,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'CompanyInfo'), 'CompanyId')),16)),1,2),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'CompanyInfo'), 'CompanyId')),16)),12,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'CompanyInfo'), 'CompanyId')),16)),10,2),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'CompanyInfo'), 'CompanyId')),16)),17,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'CompanyInfo'), 'CompanyId')),16)),15,2),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'CompanyInfo'), 'CompanyId')),16)),20,4),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'CompanyInfo'), 'CompanyId')),16)),25,12))
                     AS "ID Client",
                     JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'OrderInfo'), 'Number')
                     AS "Внутренний номер заказа Atlas",
                     parseDateTime32BestEffort(JSONExtractString(orders._airbyte_data, 'CreatedOn')) AS "DateCreatedOn",
                     if(JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'StateInfo'), 'State')='22'
                      OR JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'StateInfo'), 'State')='23', 
                      parseDateTime32BestEffortOrZero(JSONExtractString(JSONExtractRaw(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'StateInfo'), 'Audit'), 'CreatedOn')), 
                      parseDateTime32BestEffort('2035-01-01'))
                     AS "DateCompleted",
                     parseDateTime32BestEffortOrZero(JSONExtractString(JSONExtractRaw(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'StateInfo'), 'Audit'), 'CreatedOn'))
                     AS "DateCurrent",
                     JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'StateInfo'), 'State')
                     AS "Статус Atlas",
                     JSONExtractString(arrayElement(JSONExtractArrayRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'Tags'), 1), 'Value')
                     AS "Тип заказа",
                     if(JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'OrderInfo'), 'OperationType')='0', 'Доставка', 'Возврат')
                     AS "Тип доставки",
                     concat(substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'HubInfo'), 'HubId')), 16)), 7, 2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'HubInfo'), 'HubId')),16)),5,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'HubInfo'), 'HubId')),16)),3,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'HubInfo'), 'HubId')),16)),1,2),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'HubInfo'), 'HubId')),16)),12,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'HubInfo'), 'HubId')),16)),10,2),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'HubInfo'), 'HubId')),16)),17,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'HubInfo'), 'HubId')),16)),15,2),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'HubInfo'), 'HubId')),16)),20,4),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'HubInfo'), 'HubId')),16)),25,12))
                     AS "ID Hub",
                     JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'AddressInfo'), 'Town')                        
                     AS "Город разгрузки",
                     JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'AddressInfo'), 'FullAddress')
                     AS "Адрес разгрузки Atlas",
                     concat('[', 
                        JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'LocationInfo'), 'Latitude'), ',',
                        JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'LocationInfo'), 'Longitude'), ']')
                     AS "Координаты разгрузки",
                     parseDateTime32BestEffortOrZero(JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'DeliveryTimeSlot'), 'From')) 
                     AS "DeliveryFrom",
                     parseDateTime32BestEffortOrZero(JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'DeliveryTimeSlot'), 'To'))
                     AS "DeliveryTo",
                     parseDateTime32BestEffortOrZero(JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'PickupTimeSlot'), 'From'))
                     AS "PickUpFrom",
                     parseDateTime32BestEffortOrZero(JSONExtractString(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'PickupTimeSlot'), 'To'))
                     AS "PickUpTo",
                     parseDateTime32BestEffortOrZero(JSONExtractString(JSONExtractRaw(JSONExtractRaw(JSONExtractRaw(orders._airbyte_data, 'Order'), 'CourierServiceInfo'), 'Audit'), 'CreatedOn'))
                     AS "SendToCourier"
                FROM Soft._airbyte_raw_cloudOrder AS orders"""

query_5 = 'Alter Table Soft.AtlasRoute delete where "ID Route" is not null'

query_6 = """INSERT INTO Soft.AtlasRoute 
                SELECT concat(substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(routes._airbyte_data, '_id'))), 16)), 7, 2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(routes._airbyte_data, '_id'))),16)),5,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(routes._airbyte_data, '_id'))),16)),3,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(routes._airbyte_data, '_id'))),16)),1,2),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(routes._airbyte_data, '_id'))),16)),12,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(routes._airbyte_data, '_id'))),16)),10,2),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(routes._airbyte_data, '_id'))),16)),17,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(routes._airbyte_data, '_id'))),16)),15,2),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(routes._airbyte_data, '_id'))),16)),20,4),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(routes._airbyte_data, '_id'))),16)),25,12))
                     AS "ID Route",
                     concat(substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'CompanyInfo' ), 'CompanyId')), 16)), 7, 2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'CompanyInfo'), 'CompanyId')),16)),5,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'CompanyInfo'), 'CompanyId')),16)),3,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'CompanyInfo'), 'CompanyId')),16)),1,2),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'CompanyInfo'), 'CompanyId')),16)),12,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'CompanyInfo'), 'CompanyId')),16)),10,2),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'CompanyInfo'), 'CompanyId')),16)),17,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'CompanyInfo'), 'CompanyId')),16)),15,2),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'CompanyInfo'), 'CompanyId')),16)),20,4),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'CompanyInfo'), 'CompanyId')),16)),25,12))
                     AS "ID Client",
                     JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'RouteInfo'), 'Name')
                     AS "Номер маршрута",
                     parseDateTime32BestEffort(JSONExtractString(routes._airbyte_data, 'CreatedOn')) 
                     AS "DateCreatedOn",
                     parseDateTime32BestEffort(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'RouteInfo'), 'Start'))
                     AS "Старт маршрута",
                     parseDateTime32BestEffort(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'RouteInfo'), 'End'))
                     AS "Завершение маршрута",
                     JSONExtractFloat(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'RouteInfo'), 'Duration')/600000000
                     AS "Длительность маршрута",
                     concat(substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'HubInfo'), 'HubId')), 16)), 7, 2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'HubInfo'), 'HubId')),16)),5,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'HubInfo'), 'HubId')),16)),3,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'HubInfo'), 'HubId')),16)),1,2),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'HubInfo'), 'HubId')),16)),12,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'HubInfo'), 'HubId')),16)),10,2),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'HubInfo'), 'HubId')),16)),17,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'HubInfo'), 'HubId')),16)),15,2),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'HubInfo'), 'HubId')),16)),20,4),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'HubInfo'), 'HubId')),16)),25,12))
                     AS "ID Hub",
                     concat(substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'CarrierInfo'), 'CarrierId')), 16)), 7, 2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'CarrierInfo'), 'CarrierId')),16)),5,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'CarrierInfo'), 'CarrierId')),16)),3,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'CarrierInfo'), 'CarrierId')),16)),1,2),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'CarrierInfo'), 'CarrierId')),16)),12,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'CarrierInfo'), 'CarrierId')),16)),10,2),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'CarrierInfo'), 'CarrierId')),16)),17,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'CarrierInfo'), 'CarrierId')),16)),15,2),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'CarrierInfo'), 'CarrierId')),16)),20,4),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'CarrierInfo'), 'CarrierId')),16)),25,12))
                     AS "ID Carrier",
                     concat(substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(arrayElement(JSONExtractArrayRaw(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'PerformerInfo'), 'PersonIds'), 1))), 16)), 7, 2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(arrayElement(JSONExtractArrayRaw(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'PerformerInfo'), 'PersonIds'), 1))), 16)),3,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(arrayElement(JSONExtractArrayRaw(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'PerformerInfo'), 'PersonIds'), 1))), 16)),1,2),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(arrayElement(JSONExtractArrayRaw(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'PerformerInfo'), 'PersonIds'), 1))), 16)),12,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(arrayElement(JSONExtractArrayRaw(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'PerformerInfo'), 'PersonIds'), 1))), 16)),10,2),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(arrayElement(JSONExtractArrayRaw(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'PerformerInfo'), 'PersonIds'), 1))), 16)),17,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(arrayElement(JSONExtractArrayRaw(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'PerformerInfo'), 'PersonIds'), 1))), 16)),15,2),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(arrayElement(JSONExtractArrayRaw(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'PerformerInfo'), 'PersonIds'), 1))), 16)),20,4),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(arrayElement(JSONExtractArrayRaw(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'PerformerInfo'), 'PersonIds'), 1))), 16)),25,12))
                     AS "ID Person",
                     JSONExtractString(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'StateInfo'), 'State')
                     AS "Статус маршрута",
                     parseDateTime32BestEffortOrZero(JSONExtractString(JSONExtractRaw(JSONExtractRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'StateInfo'), 'Audit'), 'CreatedOn'))
                     AS "StatusDate",
                     concat(substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(arrayElement(JSONExtractArrayRaw(arrayElement(JSONExtractArrayRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'RoutePoints'), 1), 'OrderTasks'), 1), 'OrderId')), 16)), 7, 2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(arrayElement(JSONExtractArrayRaw(arrayElement(JSONExtractArrayRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'RoutePoints'), 1), 'OrderTasks'), 1), 'OrderId')), 16)),5,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(arrayElement(JSONExtractArrayRaw(arrayElement(JSONExtractArrayRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'RoutePoints'), 1), 'OrderTasks'), 1), 'OrderId')), 16)),3,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(arrayElement(JSONExtractArrayRaw(arrayElement(JSONExtractArrayRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'RoutePoints'), 1), 'OrderTasks'), 1), 'OrderId')), 16)),1,2),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(arrayElement(JSONExtractArrayRaw(arrayElement(JSONExtractArrayRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'RoutePoints'), 1), 'OrderTasks'), 1), 'OrderId')), 16)),12,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(arrayElement(JSONExtractArrayRaw(arrayElement(JSONExtractArrayRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'RoutePoints'), 1), 'OrderTasks'), 1), 'OrderId')), 16)),10,2),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(arrayElement(JSONExtractArrayRaw(arrayElement(JSONExtractArrayRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'RoutePoints'), 1), 'OrderTasks'), 1), 'OrderId')), 16)),17,2),
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(arrayElement(JSONExtractArrayRaw(arrayElement(JSONExtractArrayRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'RoutePoints'), 1), 'OrderTasks'), 1), 'OrderId')), 16)),15,2),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(arrayElement(JSONExtractArrayRaw(arrayElement(JSONExtractArrayRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'RoutePoints'), 1), 'OrderTasks'), 1), 'OrderId')), 16)),20,4),
                     '-',
                     substring(UUIDNumToString(toFixedString(base64Decode(JSONExtractString(arrayElement(JSONExtractArrayRaw(arrayElement(JSONExtractArrayRaw(JSONExtractRaw(routes._airbyte_data, 'Route'), 'RoutePoints'), 1), 'OrderTasks'), 1), 'OrderId')), 16)),25,12))
                     AS "FirstOrderID"
                FROM Soft._airbyte_raw_cloudRoute AS routes """

query_7 = 'Alter Table Soft.AtlasOrderPrices delete where "ID Order" is not null'

query_8 = """INSERT INTO Soft.AtlasOrderPrices
                SELECT 
                orders."ID Order" AS "ID Order",
                multiIf(orders."ID Client"='4b5de9c1-2222-4f57-996d-a9a600bcd4c8', '4c8dfa6d-4ea8-401b-b229-f3a16bc36a27',
                        orders."ID Client"='4b5de9c1-3117-4f57-996d-a9a600bcd4c8' AND Rcounts."Corders">=8001 AND orders."ID Order"=route."FirstOrderID", '540df5cf-2308-432e-aa65-d4e1249a8882',
                        orders."ID Client"='4b5de9c1-3117-4f57-996d-a9a600bcd4c8' AND Rcounts."Corders"<=4000 AND orders."ID Order"=route."FirstOrderID", '579928c9-de54-49d0-9c09-dd64936847da',
                        orders."ID Client"='4b5de9c1-3117-4f57-996d-a9a600bcd4c8' AND Rcounts."Corders"<=6000 AND orders."ID Order"=route."FirstOrderID", '02dd2639-ba9c-41de-844c-89e22f042ee4',
                        orders."ID Client"='f4421f9e-5962-4c68-8049-a45600f36f4b' AND counts."Corders">=11000, '1b60a186-fc07-4e7e-b6a1-176662d37da6',
                        orders."ID Client"='f4421f9e-5962-4c68-8049-a45600f36f4b' AND counts."Corders">=6600, '1cc75c35-f8d0-402a-b874-1d43b9a849d8',
                        orders."ID Client"='f4421f9e-5962-4c68-8049-a45600f36f4b' AND counts."Corders">=2200, '464a70b3-45a4-4a89-8bed-62f3540c0969',
                        orders."ID Client"='f4421f9e-5962-4c68-8049-a45600f36f4b' AND counts."Corders">=1100, 'bf1a4e0e-8692-4fe5-b9f9-192a89083a08',
                        orders."ID Client"='f4421f9e-5962-4c68-8049-a45600f36f4b' AND counts."Corders">=550, '0b5acc97-2b9f-40dc-a4c1-f4491434498b',
                        orders."ID Client"='f4421f9e-5962-4c68-8049-a45600f36f4b', '37f6ad74-441d-4b5c-be1e-91760564202a',
                        orders."ID Client"='3af19e34-2741-446f-82c0-9464eb8468b8', '0d1fb3d5-8204-4cc0-a28a-d7bde88c9b89',
                        orders."ID Client"='89cede84-6bd0-11ed-a1eb-0242ac120002', '77ca4f86-4de6-4788-aad3-77def5bfe53d',
                        orders."ID Client"='7f9d9564-9123-42fa-809c-49244df67795', 'ecbd5687-7a14-46af-99ef-c22cafb78e91',
                        orders."ID Client"='f42e39e3-abcb-47a2-a457-da60e1ea7354' AND counts."Corders">=2500, 'e5a5b04c-5337-4372-ab9a-0583d5db3fb1',
                        orders."ID Client"='f42e39e3-abcb-47a2-a457-da60e1ea7354' AND counts."Corders">=1600, '7c8d286d-0aaa-4b2d-8374-443182339e05',
                        orders."ID Client"='f42e39e3-abcb-47a2-a457-da60e1ea7354' AND counts."Corders">=1000, 'f091197a-56f6-45ea-8c0b-ac16aca83199',
                        orders."ID Client"='f42e39e3-abcb-47a2-a457-da60e1ea7354', '634365b1-fa9f-410f-beb7-686a8cde78e9',
                        NULL)
                AS "ID Price",
                multiIf(orders."ID Client"='4b5de9c1-2222-4f57-996d-a9a600bcd4c8', (SELECT "Стоимость услуг Atlas за перевозку заказа c НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='4c8dfa6d-4ea8-401b-b229-f3a16bc36a27'),
                        orders."ID Client"='4b5de9c1-3117-4f57-996d-a9a600bcd4c8' AND Rcounts."Corders">=8001 AND orders."ID Order"=route."FirstOrderID", (SELECT "Стоимость услуг Atlas за перевозку заказа c НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='540df5cf-2308-432e-aa65-d4e1249a8882'),
                        orders."ID Client"='4b5de9c1-3117-4f57-996d-a9a600bcd4c8' AND Rcounts."Corders"<=4000 AND orders."ID Order"=route."FirstOrderID", (SELECT "Стоимость услуг Atlas за перевозку заказа c НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='579928c9-de54-49d0-9c09-dd64936847da'),
                        orders."ID Client"='4b5de9c1-3117-4f57-996d-a9a600bcd4c8' AND Rcounts."Corders"<=6000 AND orders."ID Order"=route."FirstOrderID", (SELECT "Стоимость услуг Atlas за перевозку заказа c НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='02dd2639-ba9c-41de-844c-89e22f042ee4'),
                        orders."ID Client"='f4421f9e-5962-4c68-8049-a45600f36f4b' AND counts."Corders">=11000, (SELECT "Стоимость услуг Atlas за перевозку заказа c НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='1b60a186-fc07-4e7e-b6a1-176662d37da6'),
                        orders."ID Client"='f4421f9e-5962-4c68-8049-a45600f36f4b' AND counts."Corders">=6600, (SELECT "Стоимость услуг Atlas за перевозку заказа c НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='1cc75c35-f8d0-402a-b874-1d43b9a849d8'),
                        orders."ID Client"='f4421f9e-5962-4c68-8049-a45600f36f4b' AND counts."Corders">=2200, (SELECT "Стоимость услуг Atlas за перевозку заказа c НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='464a70b3-45a4-4a89-8bed-62f3540c0969'),
                        orders."ID Client"='f4421f9e-5962-4c68-8049-a45600f36f4b' AND counts."Corders">=1100, (SELECT "Стоимость услуг Atlas за перевозку заказа c НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='bf1a4e0e-8692-4fe5-b9f9-192a89083a08'),
                        orders."ID Client"='f4421f9e-5962-4c68-8049-a45600f36f4b' AND counts."Corders">=550, (SELECT "Стоимость услуг Atlas за перевозку заказа c НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='0b5acc97-2b9f-40dc-a4c1-f4491434498b'),
                        orders."ID Client"='f4421f9e-5962-4c68-8049-a45600f36f4b', (SELECT "Стоимость услуг Atlas за перевозку заказа c НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='37f6ad74-441d-4b5c-be1e-91760564202a'),
                        orders."ID Client"='3af19e34-2741-446f-82c0-9464eb8468b8', (SELECT "Стоимость услуг Atlas за перевозку заказа c НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='0d1fb3d5-8204-4cc0-a28a-d7bde88c9b89'),
                        orders."ID Client"='89cede84-6bd0-11ed-a1eb-0242ac120002', (SELECT "Стоимость услуг Atlas за перевозку заказа c НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='77ca4f86-4de6-4788-aad3-77def5bfe53d'),
                        orders."ID Client"='7f9d9564-9123-42fa-809c-49244df67795', (SELECT "Стоимость услуг Atlas за перевозку заказа c НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='ecbd5687-7a14-46af-99ef-c22cafb78e91'),
                        orders."ID Client"='f42e39e3-abcb-47a2-a457-da60e1ea7354' AND counts."Corders">=2500, (SELECT "Стоимость услуг Atlas за перевозку заказа c НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='e5a5b04c-5337-4372-ab9a-0583d5db3fb1'),
                        orders."ID Client"='f42e39e3-abcb-47a2-a457-da60e1ea7354' AND counts."Corders">=1600, (SELECT "Стоимость услуг Atlas за перевозку заказа c НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='7c8d286d-0aaa-4b2d-8374-443182339e05'),
                        orders."ID Client"='f42e39e3-abcb-47a2-a457-da60e1ea7354' AND counts."Corders">=1000, (SELECT "Стоимость услуг Atlas за перевозку заказа c НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='f091197a-56f6-45ea-8c0b-ac16aca83199'),
                        orders."ID Client"='f42e39e3-abcb-47a2-a457-da60e1ea7354', (SELECT "Стоимость услуг Atlas за перевозку заказа c НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='634365b1-fa9f-410f-beb7-686a8cde78e9'),
                        NULL)
                AS "withNDS",
                multiIf(orders."ID Client"='4b5de9c1-2222-4f57-996d-a9a600bcd4c8', (SELECT "Стоимость услуг Atlas за перевозку заказа без НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='4c8dfa6d-4ea8-401b-b229-f3a16bc36a27'),
                        orders."ID Client"='4b5de9c1-3117-4f57-996d-a9a600bcd4c8' AND Rcounts."Corders">=8001 AND orders."ID Order"=route."FirstOrderID", (SELECT "Стоимость услуг Atlas за перевозку заказа без НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='540df5cf-2308-432e-aa65-d4e1249a8882'),
                        orders."ID Client"='4b5de9c1-3117-4f57-996d-a9a600bcd4c8' AND Rcounts."Corders"<=4000 AND orders."ID Order"=route."FirstOrderID", (SELECT "Стоимость услуг Atlas за перевозку заказа без НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='579928c9-de54-49d0-9c09-dd64936847da'),
                        orders."ID Client"='4b5de9c1-3117-4f57-996d-a9a600bcd4c8' AND Rcounts."Corders"<=6000 AND orders."ID Order"=route."FirstOrderID", (SELECT "Стоимость услуг Atlas за перевозку заказа без НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='02dd2639-ba9c-41de-844c-89e22f042ee4'),
                        orders."ID Client"='f4421f9e-5962-4c68-8049-a45600f36f4b' AND counts."Corders">=11000, (SELECT "Стоимость услуг Atlas за перевозку заказа без НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='1b60a186-fc07-4e7e-b6a1-176662d37da6'),
                        orders."ID Client"='f4421f9e-5962-4c68-8049-a45600f36f4b' AND counts."Corders">=6600, (SELECT "Стоимость услуг Atlas за перевозку заказа без НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='1cc75c35-f8d0-402a-b874-1d43b9a849d8'),
                        orders."ID Client"='f4421f9e-5962-4c68-8049-a45600f36f4b' AND counts."Corders">=2200, (SELECT "Стоимость услуг Atlas за перевозку заказа без НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='464a70b3-45a4-4a89-8bed-62f3540c0969'),
                        orders."ID Client"='f4421f9e-5962-4c68-8049-a45600f36f4b' AND counts."Corders">=1100, (SELECT "Стоимость услуг Atlas за перевозку заказа без НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='bf1a4e0e-8692-4fe5-b9f9-192a89083a08'),
                        orders."ID Client"='f4421f9e-5962-4c68-8049-a45600f36f4b' AND counts."Corders">=550, (SELECT "Стоимость услуг Atlas за перевозку заказа без НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='0b5acc97-2b9f-40dc-a4c1-f4491434498b'),
                        orders."ID Client"='f4421f9e-5962-4c68-8049-a45600f36f4b', (SELECT "Стоимость услуг Atlas за перевозку заказа без НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='37f6ad74-441d-4b5c-be1e-91760564202a'),
                        orders."ID Client"='3af19e34-2741-446f-82c0-9464eb8468b8', (SELECT "Стоимость услуг Atlas за перевозку заказа без НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='0d1fb3d5-8204-4cc0-a28a-d7bde88c9b89'),
                        orders."ID Client"='89cede84-6bd0-11ed-a1eb-0242ac120002', (SELECT "Стоимость услуг Atlas за перевозку заказа без НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='77ca4f86-4de6-4788-aad3-77def5bfe53d'),
                        orders."ID Client"='7f9d9564-9123-42fa-809c-49244df67795', (SELECT "Стоимость услуг Atlas за перевозку заказа без НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='ecbd5687-7a14-46af-99ef-c22cafb78e91'),
                        orders."ID Client"='f42e39e3-abcb-47a2-a457-da60e1ea7354' AND counts."Corders">=2500, (SELECT "Стоимость услуг Atlas за перевозку заказа без НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='e5a5b04c-5337-4372-ab9a-0583d5db3fb1'),
                        orders."ID Client"='f42e39e3-abcb-47a2-a457-da60e1ea7354' AND counts."Corders">=1600, (SELECT "Стоимость услуг Atlas за перевозку заказа без НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='7c8d286d-0aaa-4b2d-8374-443182339e05'),
                        orders."ID Client"='f42e39e3-abcb-47a2-a457-da60e1ea7354' AND counts."Corders">=1000, (SELECT "Стоимость услуг Atlas за перевозку заказа без НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='f091197a-56f6-45ea-8c0b-ac16aca83199'),
                        orders."ID Client"='f42e39e3-abcb-47a2-a457-da60e1ea7354', (SELECT "Стоимость услуг Atlas за перевозку заказа без НДС" FROM Soft.AtlasDeliveryPrice WHERE "ID Price"='634365b1-fa9f-410f-beb7-686a8cde78e9'),
                        NULL)
                AS "withoutNDS",
                NULL AS "other"
            FROM Soft.AtlasOrder AS orders 
            LEFT JOIN Soft.AtlasRoute AS route ON orders."ID Order"=route."FirstOrderID" 
            LEFT JOIN (SELECT date_trunc('month', "DateCreatedOn") AS "month", count() AS "Corders", "ID Client" AS "Client"
                FROM Soft.AtlasOrder GROUP BY date_trunc('month', "DateCreatedOn"), "ID Client") AS counts on counts."month"=date_trunc('month', AtlasOrder."DateCreatedOn") AND counts."Client"=orders."ID Client" 
            LEFT JOIN (SELECT date_trunc('month', "DateCreatedOn") AS "month", count() AS "routes", "ID Client" AS "Client"
                FROM Soft.AtlasOrder GROUP BY date_trunc('month', "DateCreatedOn"), "ID Client") AS Rcounts on Rcounts."month"=date_trunc('month', AtlasOrder."DateCreatedOn") AND Rcounts."Client"=orders."ID Client" """

query_9 = "Alter Table Soft.SoftDelivery delete where ID is not null"

query_10 = """INSERT INTO Soft.SoftDelivery
                SELECT orders."ID Order" AS "ID",
                orders."ID Client" AS "ID Client",
                prices."ID Price" AS "ID Price",
                Cprice."ID Courier Price" AS "ID Courier Price",
                orders."Внутренний номер заказа Atlas" AS "Номер заказа Atlas",
                couriers."Номер заказа курьерской службы" AS "Номер заказа КС",
                multiIf(orders."Тип заказа"='yataxi', 'C2C',
                        orders."Тип заказа"='Dostavista', 'C2C',
                        orders."Тип заказа"='Bringo', 'C2C',
                        orders."Тип заказа"='CnC', 'CnC', 
                        'Delivery')
                AS "Тип заказа",
                multiIf(orders."Тип заказа"='yataxi', 'Яндекс',
                        orders."Тип заказа"='Dostavista', 'Достависта',
                        orders."Тип заказа"='Bringo', 'Бринго',
                        couriers."Курьерская служба") 
                AS "Курьерская служба",
                client."Клиент Atlas" AS "Клиент",
                client."ИНН Клиента Atlas" AS "ИНН Клиента",
                client."Регион клиента" AS "Регион клиента",
                client."Адрес клиента" AS "Адрес клиента",
                client."Телефон" AS "Телефон клиента",
                multiIf(orders."Статус Atlas"='22', 'Завершён',
                        orders."Статус Atlas"='23', 'Отменён',
                        orders."Статус Atlas"='6', 'Не геокодирован',
                        orders."Статус Atlas"='7', 'Распределяется',
                        orders."Статус Atlas"='8', 'Распределён',
                        orders."Статус Atlas"='9', 'Не распределён',
                        orders."Статус Atlas"='11', 'Время корректно',
                        orders."Статус Atlas"='13', 'Готов к планированию',
                        orders."Статус Atlas"='14', 'Готов к назначению',
                        orders."Статус Atlas"='15', 'Планирование',
                        orders."Статус Atlas"='16', 'Запланирована',
                        orders."Статус Atlas"='17', 'Погружен',
                        orders."Статус Atlas"='18', 'В пути',
                        orders."Статус Atlas"='20', 'Прибыл',
                        orders."Статус Atlas"='21', 'Начат',
                        orders."Статус Atlas"='24', 'Отложен',
                        orders."Статус Atlas"='25', 'Не спланирован',
                        orders."Статус Atlas")   
                AS "Статус заказа",
                orders."Тип доставки" AS "Тип доставки",
                hubs."Клиент Atlas" AS "Зона доставки",
                orders."DateCreatedOn" AS CreatedOn,
                orders."DateCompleted" AS CompletedOn,
                hubs."Адрес клиента" AS "Адрес загрузки",
                if(orders."Тип доставки"='Доставка', NULL, orders."Адрес разгрузки Atlas")
                AS "Адрес промежуточный",
                if(orders."Тип доставки"='Доставка', orders."Адрес разгрузки Atlas", hubs."Адрес клиента")
                AS "Адрес разгрузки",
                hubs."Регион клиента" AS "Город погрузки",
                orders."Город разгрузки" AS "Город разгрузки",
                couriers."Стоимость курьерской доставки без НДС" AS "Стоимость КС без НДС",
                prices."withoutNDS" AS "Стоимость Atlas Delivery без НДС",
                Cprice."Стоимость услуг Atlas за перевозку заказа без НДС" AS "Стоимость Atlas Courier без НДС",
                NULL AS "Стоимость Atlas прочее без НДС",
                plus(prices."withoutNDS", assumeNotNull(Cprice."Стоимость услуг Atlas за перевозку заказа без НДС")) 
                AS "Сумма Atlas без НДС",
                couriers."Стоимость курьерской доставки с НДС" AS "Стоимость КС с НДС",
                prices."withNDS" AS "Стоимость Atlas Delivery с НДС",
                Cprice."Стоимость услуг Atlas за перевозку заказа с НДС" AS "Стоимость Atlas Courier с НДС",
                NULL AS "Стоимость Atlas прочее с НДС",
                plus(prices."withNDS", assumeNotNull(Cprice."Стоимость услуг Atlas за перевозку заказа с НДС")) 
                AS "Сумма Atlas с НДС",
                if(orders."Тип доставки"='Доставка', orders."DeliveryFrom", orders."PickUpFrom") 
                AS "OrderTimeslotFrom",
                if(orders."Тип доставки"='Доставка', orders."DeliveryTo", orders."PickUpTo") 
                AS "OrderTimeslotTo",
                multiIf(orders."Тип доставки"='Доставка' AND orders."DateCompleted"<orders."DeliveryTo", 'Да',
                    orders."Тип доставки"='Доставка' AND orders."DateCompleted">orders."DeliveryTo", 'Нет',
                    orders."Тип доставки"='Возврат' AND orders."DateCompleted"<orders."PickUpTo", 'Да',
                    orders."Тип доставки"='Возврат' AND orders."DateCompleted">orders."PickUpTo", 'Нет',
                    NULL) 
                AS "Intime",
                hubs."Агенство" AS "Агенство",
                hubs."Координаты клиента" AS "Координаты загрузки",
                if(orders."Тип доставки"='Доставка', NULL, orders."Координаты разгрузки")
                AS "Координаты промежуточные",
                if(orders."Тип доставки"='Доставка', orders."Координаты разгрузки", hubs."Координаты клиента")
                AS "Координаты разгрузки",
                toTimeZone(orders."SendToCourier", 'Europe/Moscow') AS "Передано в КС",
                routes."Номер маршрута" AS "Номер маршрута",
                routes."DateCreatedOn" AS "RouteCreatedOn",
                routes."Старт маршрута" AS "Старт маршрута",
                routes."Завершение маршрута" AS "Завершение маршрута",
                routes."Длительность маршрута" AS "Длительность маршрута минут",
                carriers."Клиент Atlas" AS "Транспортная компания",
                routes."ID Person" AS "Водитель",
                multiIf(routes."Статус маршрута"='27', 'Завершён',
                        routes."Статус маршрута"='25', 'Выполнен',
                        routes."Статус маршрута"='20', 'Начат',
                        routes."Статус маршрута"='19', 'Погрузка завершена',
                        routes."Статус маршрута"='16', 'Прибыл на погрузку',
                        routes."Статус маршрута"='9', 'Не распределён',
                        routes."Статус маршрута"='12', 'Отправлен водителю',
                        routes."Статус маршрута"='11', 'Водитель назначен',
                        routes."Статус маршрута"='4', 'ТК назначена',
                        routes."Статус маршрута"='3', 'Спланирован',
                        routes."Статус маршрута"='2', 'Не спланирован',
                        routes."Статус маршрута"='1', 'Планирование',
                        routes."Статус маршрута"='0', 'Создан',
                        routes."Статус маршрута")   
                AS "Статус маршрута",
                routes."StatusDate" AS "Время статуса маршрута"
            FROM Soft.AtlasOrder AS orders
            LEFT JOIN Soft.Clients AS client on orders."ID Client"=client."ID Client"
            LEFT JOIN Soft.Clients AS hubs on orders."ID Hub"=hubs."ID Client"
            LEFT JOIN Soft.AtlasCourierPrice AS Cprice on orders."ID Client"=Cprice."ID Client"
            LEFT JOIN Soft.AtlasOrderPrices AS prices on orders."ID Order"=prices."ID Order"
            LEFT JOIN Soft.AtlasRoute AS routes on orders."ID Order"=routes."FirstOrderID"
            LEFT JOIN Soft.Clients AS carriers on routes."ID Carrier"=carriers."ID Client"
            LEFT JOIN (
               SELECT
                       Soft.CourierServicesReports."ID Order" AS "ID Order",
                       MIN(Soft.CourierServicesReports."Номер заказа курьерской службы") AS "Номер заказа курьерской службы",
                       MIN(Soft.CourierServicesReports."Курьерская служба") AS "Курьерская служба",
                       SUM(Soft.CourierServicesReports."Стоимость курьерской доставки без НДС") AS "Стоимость курьерской доставки без НДС",
                       SUM(Soft.CourierServicesReports."Стоимость курьерской доставки с НДС") AS "Стоимость курьерской доставки с НДС"
               FROM Soft.CourierServicesReports
               GROUP BY Soft.CourierServicesReports."ID Order"
           ) AS couriers on orders."ID Order"=couriers."ID Order" """

