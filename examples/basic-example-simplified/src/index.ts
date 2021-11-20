import { config } from 'dotenv';
config({ path: 'env.local' });

import { ColorConsole } from './color-console';

import { Session } from '@ggvlasov/ydb-sdk';
import { driver, initYDBdriver, describeTable, logger, databaseName } from './ydb-functions';
import { fillTmdbWithData } from './fill_tmdb_with_data';
import { createTables } from './create-table';
import { TMDB_TABLE } from './data-helpers';

(async function run() {
    await initYDBdriver(); // если не удалось инициализация - то внутри идет process.exit

    await driver.tableClient.withSession(async (session) => {
        await createTables(session, logger);

        console.log(ColorConsole.FgBlue, '->->->-> fillTablesWithData START', ColorConsole.Reset);
        await fillTmdbWithData(session, logger);
        console.log(ColorConsole.FgBlue, '->->->-> fillTablesWithData END', ColorConsole.Reset);
        console.log();
        await describeTable(session, TMDB_TABLE, logger);
    });

    await driver.destroy();
})();

// const text = await run();
