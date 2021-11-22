process.env.YDB_SDK_PRETTY_LOGS = '1';

import { initYDBdriver, driver, logger } from '../type-utils/ydb-functions';

async function run(): Promise<void> {
    logger.info('Testing scheme client capabilities...');
    await driver.schemeClient.makeDirectory('example-path');
    await driver.schemeClient.makeDirectory('example-path/subpath');
    await driver.schemeClient.modifyPermissions('example-path/subpath', [
        {
            grant: {
                subject: 'tsufiev@staff',
                permissionNames: ['read', 'use'],
            },
        },
    ]);
    const entry = await driver.schemeClient.describePath('example-path');
    const children = await driver.schemeClient.listDirectory('example-path');
    logger.info(`Created path: ${JSON.stringify(entry, null, 2)}`);
    logger.info(`Path contents: ${JSON.stringify(children, null, 2)}`);
    await driver.schemeClient.removeDirectory('example-path/subpath');
    await driver.schemeClient.removeDirectory('example-path');
}

(async () => {
    await initYDBdriver();
    await run();
})();
