import { config } from 'dotenv';
config({ path: 'env.local' });

// import { Driver, Logger, Session, Ydb } from 'ydb-sdk';
import { Session } from 'ydb-sdk';
import {
  driver,
  initYDBdriver,
  describeTable,
  logger,
  databaseName,
} from './ydb/ydb-functions';
import { fillTmdbWithData } from './ydb/fill_tmdb_with_data';
import { fillFromStruct } from './ydb/fill_struct_data';
import { Tdef } from './ydb/struct-to-class';

(async function run() {
  await initYDBdriver(); // если не удалось инициализация - то внутри идет process.exit

  await driver.tableClient.withSession(async (session) => {
    // проверим прошла ли инициализация драйвера
    // await describeTable(session, 'series', logger);
    // await describeTable(session, 'tmdb', logger);
    /*  console.log('+=+=+=+= describeTable START');
    console.log('+=+=+=+= describeTable END');

    console.log('->->->-> fillTablesWithData START');
    await fillTmdbWithData(session, logger);
    console.log('->->->-> fillTablesWithData END');*/
    // console.log('->->->-> fillFromStruct START');
    // await fillFromStruct(session, logger);
    // console.log('->->->-> fillFromStruct END');

    // await Tdef.dropDBTable(session, logger);
    // await Tdef.createDBTable(session, logger);

    /*const r1 = new Tdef({ id: 85, title: '85' });
    await r1.upsertToDB(session, logger);
    r1.id = 95;
    r1.title = '95';
    await r1.upsertToDB(session, logger);*/

    const data: Array<Tdef> = [];
    data.push(
      new Tdef({
        id: 11,
        title: '11 asd asd asd',
        adult: false,
        backdrop_path: 'backdrop_path',
      })
    );
    data.push(
      new Tdef({
        id: 22,
        title: '22 asdf',
      })
    );
    await Tdef.upsertSeriesToDB(session, logger, data);

    /*
    // executeBulkUpsert
    const res = await session.executeBulkUpsert(
      databaseName + '/tmdb',
      Tdef.asTypedCollection(data)
    );
    console.log(res);
    // @ts-ignore
    console.log(res.operation.issues);
*/
  });

  await driver.destroy();
})();

// const text = await run();
