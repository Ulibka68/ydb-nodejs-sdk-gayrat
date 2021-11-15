import { config } from 'dotenv';
config({ path: 'env.local' });

const databaseName =process.env.DATABASENAME;


import {
    Column,
    getLogger,
    Driver,
    getCredentialsFromEnv,
    Logger,
    Session,
    withRetries,
    Ydb,
} from 'ydb-sdk';

const logger = getLogger({ level: 'debug' });
const entryPoint = 'grpcs://ydb.serverless.yandexcloud.net:2135';
// const dbName = '/ru-central1/b1gib03pgvqrrfvhl3kb/etnnr4j3s2malltd5a4t';

async function describeTable(
    session: Session,
    tableName: string,
    logger: Logger
) {
    logger.info(`Describing table: ${tableName}`);
    const result = await session.describeTable(tableName);
    for (const column of result.columns) {
        console.log(
            `Column name '${column.name}' has type ${JSON.stringify(column.type)}`
        );
        console.log(column.type);
        console.log(column.type);
        console.log(column.type!.optionalType!.item!.typeId);
    }
}

async function run() {
    const authService = getCredentialsFromEnv(entryPoint, databaseName, logger);
    const driver = new Driver(entryPoint, databaseName, authService);

    if (!(await driver.ready(10000))) {
        logger.fatal(`Driver has not become ready in 10 seconds!`);
        process.exit(1);
    }
    console.log('driver ready');
    // return;

    await driver.tableClient.withSession(async (session) => {
        console.log('+=+=+=+= describeTable START');
        await describeTable(session, 'series', logger);
        console.log('+=+=+=+= describeTable END');

        console.log('->->->-> fillTablesWithData START');
        await importTmdb(session, logger);
        console.log('->->->-> fillTablesWithData END');
        // выполняем запросы в конкретной сессии
        /*
        console.log('=== createTables START');
        await createTables(session, logger);
        console.log('=== createTables END');
        console.log('+=+=+=+= describeTable START');
        await describeTable(session, 'series', logger);
        console.log('+=+=+=+= describeTable END');
        console.log('->->->-> fillTablesWithData START');
        await fillTablesWithData(dbName, session, logger);
        console.log('->->->-> fillTablesWithData END');
        await fillTablesWithData2(dbName, session, logger);

         */
    });

    await driver.destroy();
}

run();
