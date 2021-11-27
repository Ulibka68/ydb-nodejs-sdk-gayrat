import { Logger, Session, withRetries, Ydb } from 'ydb-sdk';
import { databaseName } from './ydb-functions';
import { Tdef } from './struct-to-class';

/*
PRAGMA

AutoCommit	Автоматически выполнять COMMIT после каждого запроса.

TablePathPrefix
    Добавить указанный префикс к путям таблиц внутри кластеров.
    Работает по принципу объединения путей в файловой системе: поддерживает ссылки на родительский каталог .. и не требует добавления / справа.

    Пример
    PRAGMA TablePathPrefix = "home/yql"; SELECT * FROM test;

    Префикс не добавляется, если имя таблицы указано как абсолютный путь (начинается с /).
*/

export async function fillFromStruct(session: Session, logger: Logger) {
  const tmdb_record = new Tdef({
    id: 2213,
    title: 'title 2213',
    genre_ids: JSON.stringify([1]),
    release_date: new Date(),
    popularity: 1,
    poster_path: 'poster_path',
    video: true,
  });

  async function fillTable() {
    logger.info('Inserting data to tables, preparing query...');
    // console.log(query);
    let preparedQuery: Ydb.Table.PrepareQueryResult;
    try {
      preparedQuery = await session.prepareQuery(Tdef.refMetaData.YQLUpsert);
    } catch (err) {
      if (err instanceof Error) {
        console.error(err.message);
        process.exit(55);
      }
    }
    logger.info('Query has been prepared, executing...');
    console.log(
      "series.getTypedValue('seriesId'),",
      tmdb_record.getTypedValue('id')
    );
    console.log(
      "series.getTypedValue('title')",
      tmdb_record.getTypedValue('title')
    );
    console.log(
      "getTypedValue('genre_ids')",
      tmdb_record.getTypedValue('genre_ids')
    );
    console.log(
      "getTypedValue('release_date')",
      tmdb_record.getTypedValue('release_date')
    );
    // @ts-ignore
    await session.executeQuery(preparedQuery, {
      $id: tmdb_record.getTypedValue('id'),
      $title: tmdb_record.getTypedValue('title'),
      $genre_ids: tmdb_record.getTypedValue('genre_ids'),
      $release_date: tmdb_record.getTypedValue('release_date'),
    });
  }

  await withRetries(fillTable);
}
