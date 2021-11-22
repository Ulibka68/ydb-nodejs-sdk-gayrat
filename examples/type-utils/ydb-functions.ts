import { config } from 'dotenv';
config({ path: 'env.local' });

import { getLogger, Driver, getCredentialsFromEnv, Logger, Session, primitiveTypeIdToName } from '@ggvlasov/ydb-sdk';
import { ColorConsole } from './color-console';

export const databaseName = process.env.DATABASENAME!;
export const logger = getLogger({ level: process.env.LOGLEVEL! });
export const entryPoint = process.env.ENTRYPOINT!;
export let driver: Driver = null as unknown as Driver; // singleton

export async function initYDBdriver() {
    if (driver) return; // singleton
    logger.info('Start preparing driver ...');
    const authService = getCredentialsFromEnv(entryPoint, databaseName, logger);
    driver = new Driver(entryPoint, databaseName, authService);

    if (!(await driver.ready(10000))) {
        logger.fatal(`Driver has not become ready in 10 seconds!`);
        process.exit(1);
    }
    return driver;
}

export async function describeTable(session: Session, tableName: string, logger: Logger) {
    logger.info(`Describing table: ${tableName}`);
    const result = await session.describeTable(tableName, {}, { includeTableStats: true });
    // const result = await session.describeTable(tableName);
    console.log(
        ColorConsole.FgBlue + '%s',
        `\nDescribe table ${ColorConsole.FgRed} "${tableName}"${ColorConsole.FgBlue}`
    );
    for (const column of result.columns) {
        console.log(
            `  Column '${column.name}' type ${primitiveTypeIdToName[column.type!.optionalType!.item!.typeId!]}`
        );
    }

    console.log(ColorConsole.Reset);
    console.log(
        '  storeSize : "',
        new Intl.NumberFormat().format((result?.tableStats?.storeSize as Long).low),
        '" байт'
    );
    console.log('  partitions : ', (result?.tableStats?.partitions as Long).low);
    console.log('  rowsEstimate : ', (result?.tableStats?.rowsEstimate as Long).low);
    // console.log('  modificationTime nanos : ', result?.tableStats?.modificationTime?.nanos);
    // console.log('  modificationTime seconds : ', (result?.tableStats?.modificationTime?.seconds as Long).low);
    console.log(
        '  modificationTime : ',
        new Date((result?.tableStats?.modificationTime?.seconds as Long).low * 1000),
        '\n'
    );
}
