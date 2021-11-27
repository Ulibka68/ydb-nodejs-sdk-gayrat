import { driver, initYDBdriver, logger } from '../type-utils/ydb-functions';
import { Series, Season, Episode } from './table-definitions';
import { getSeriesData, getSeasonsData, getEpisodesData } from './table-data';

(async function run() {
    await initYDBdriver(); // если не удалось инициализация - то внутри идет process.exit

    await driver.tableClient.withSession(async (session) => {
        await Series.upsertSeriesToDB(session, logger, getSeriesData());
        await Season.upsertSeriesToDB(session, logger, getSeasonsData());
        await Episode.upsertSeriesToDB(session, logger, getEpisodesData());
    });

    await driver.destroy();
})();
