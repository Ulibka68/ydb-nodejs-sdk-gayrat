import {
  getLogger,
  Driver,
  getCredentialsFromEnv,
  Logger,
  Session,
  primitiveTypeIdToName,
} from 'ydb-sdk';

export const databaseName = process.env.DATABASENAME!;
export const logger = getLogger({ level: process.env.LOGLEVEL! });
export const entryPoint = process.env.ENTRYPOINT!;
export let driver: Driver = null as unknown as Driver; // singletone

export async function describeTable(
  session: Session,
  tableName: string,
  logger: Logger
) {
  logger.info(`Describing table: ${tableName}`);
  const result = await session.describeTable(tableName);
  console.log('\nTable ', tableName);
  for (const column of result.columns) {
    // console.log(
    //   `Column name '${column.name}' has type ${JSON.stringify(column.type)}`
    // );
    console.log(
      `  Column '${column.name}' type ${
        primitiveTypeIdToName[column.type!.optionalType!.item!.typeId!]
      }`
    );
  }
  console.log();
}

export async function initYDBdriver() {
  if (driver) return; // singletone
  logger.info('Start preparing driver ...');
  const authService = getCredentialsFromEnv(entryPoint, databaseName, logger);
  driver = new Driver(entryPoint, databaseName, authService);

  if (!(await driver.ready(10000))) {
    logger.fatal(`Driver has not become ready in 10 seconds!`);
    process.exit(1);
  }
  logger.info('Driver ready');
  return driver;
}
