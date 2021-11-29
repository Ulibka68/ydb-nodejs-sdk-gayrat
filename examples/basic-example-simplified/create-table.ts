import { Session, Logger, TableDescription, Column, Ydb } from '@ggvlasov/ydb-sdk';
import { TMDB_TABLE } from './data-helpers';

export async function createTables(session: Session, logger: Logger) {
    logger.info('Dropping old tables...');
    await session.dropTable(TMDB_TABLE);

    logger.info('Creating tables...');
    await session.createTable(
        TMDB_TABLE,
        new TableDescription()
            .withColumn(
                new Column(
                    'id',
                    Ydb.Type.create({ optionalType: { item: { typeId: Ydb.Type.PrimitiveTypeId.UINT64 } } })
                )
            )
            .withColumn(
                new Column(
                    'title',
                    Ydb.Type.create({ optionalType: { item: { typeId: Ydb.Type.PrimitiveTypeId.UTF8 } } })
                )
            )
            .withColumn(
                new Column(
                    'release_date',
                    Ydb.Type.create({ optionalType: { item: { typeId: Ydb.Type.PrimitiveTypeId.DATE } } })
                )
            )
            .withColumn(
                new Column(
                    'genre_ids',
                    Ydb.Type.create({ optionalType: { item: { typeId: Ydb.Type.PrimitiveTypeId.JSON } } })
                )
            )
            .withPrimaryKey('id')
    );
}
