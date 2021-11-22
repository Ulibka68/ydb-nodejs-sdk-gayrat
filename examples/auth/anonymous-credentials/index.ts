import { config } from 'dotenv';
config({ path: 'env.local' });

import { AnonymousAuthService, Driver, getLogger } from '@ggvlasov/ydb-sdk';

export const databaseName = process.env.DATABASENAME!;
export const logger = getLogger({ level: process.env.LOGLEVEL! });
export const entryPoint = process.env.ENTRYPOINT!;
export const driver: Driver = null as unknown as Driver; // singleton

export async function run() {
    logger.debug('Driver initializing...');

    const driver = new Driver(entryPoint, databaseName, new AnonymousAuthService());
    const timeout = 10000;
    if (!(await driver.ready(timeout))) {
        logger.fatal(`Driver has not become ready in ${timeout}ms!`);
        process.exit(1);
    }
}

(async () => {
    await run();
})();
