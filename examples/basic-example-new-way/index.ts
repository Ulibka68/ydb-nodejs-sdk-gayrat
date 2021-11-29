import { driver, initYDBdriver, logger, databaseName } from '../type-utils/ydb-functions';
import { Series, Season, Episodes } from './table-definitions';
import { getSeriesData, getSeasonsData, getEpisodesData, getSeasonsData2 } from './table-data';

(async function run() {
    await initYDBdriver(); // если не удалось инициализация - то внутри идет process.exit

    console.log('\n\nтаблица Episodes, данные из Episodes.refMetaData\n');
    console.log('+++++++++++++++++++ YQLCreateTable');
    console.log(Episodes.refMetaData.YQLCreateTable);
    // console.log('+++++++++++++++++++');
    // console.log(Episodes.refMetaData.fieldsDescriptions);
    console.log('+++++++++++++++++++ YQLUpsert');
    console.log(Episodes.refMetaData.YQLUpsert);
    console.log('+++++++++++++++++++ YQLReplaceSeries');
    console.log(Episodes.refMetaData.YQLReplaceSeries);
    console.log('+++++++++++++++++++\n\n');

    await driver.tableClient.withSession(async (session) => {
        await Series.dropDBTable(session, logger);
        await Series.createDBTable(session, logger);
        await Series.replaceSeriesToDB(session, logger, getSeriesData());

        await Season.dropDBTable(session, logger);
        await Season.createDBTable(session, logger);
        await Season.replaceSeriesToDB(session, logger, getSeasonsData());

        await Episodes.dropDBTable(session, logger);
        await Episodes.createDBTable(session, logger);
        await Episodes.replaceSeriesToDB(session, logger, getEpisodesData());
    });

    console.log('-------- BULK UPSERT ---------------');
    await driver.tableClient.withSession(async (session) => {
        // executeBulkUpsert
        const res = await session.executeBulkUpsert(
            databaseName + '/season',
            Season.asTypedCollection(getSeasonsData2())
        );
        console.log('Возвращаемое значение из опреации: ');
        console.log(res);
        // console.log(res?.operation?.issues);
    });

    await driver.destroy();
})();
