import 'reflect-metadata';
import { Ydb, TypedData, YdbTableMetaData, ConvertStructToTypes } from '@ggvlasov/ydb-sdk';
import { databaseName } from '../type-utils/ydb-functions';

const Pt = Ydb.Type.PrimitiveTypeId;

const series = {
    seriesId: { val: 0, pt: Pt.UINT64, opt: 'r', pk: true },
    title: { val: 'title', pt: Pt.UTF8, opt: 0 },
    release_date: { val: new Date(), pt: Pt.DATE, opt: 0 },
    seriesInfo: { val: 'title', pt: Pt.UTF8, opt: 0 },
};

export type ISeries = ConvertStructToTypes<typeof series>;

export class Series extends TypedData {
    public static refMetaData: YdbTableMetaData;

    constructor(data: ISeries) {
        super(data);
    }
}

const episode = {
    seriesId: { val: 0, pt: Pt.UINT64, opt: 'r', pk: true },
    seasonId: { val: 0, pt: Pt.UINT64, opt: 'r', pk: true },
    episodeId: { val: 0, pt: Pt.UINT64, opt: 'r', pk: true },
    airDate: { val: new Date(), pt: Pt.DATE, opt: 0 },
    title: { val: 'title', pt: Pt.UTF8, opt: 0 },
};

export type IEpisodes = ConvertStructToTypes<typeof episode>;

export class Episodes extends TypedData {
    public static refMetaData: YdbTableMetaData;

    constructor(data: IEpisodes) {
        super(data);
    }
}

const season = {
    seriesId: { val: 0, pt: Pt.UINT64, opt: 0 },
    seasonId: { val: 0, pt: Pt.UINT64, opt: 'r', pk: true },
    title: { val: 'title', pt: Pt.UTF8, opt: 0 },
    firstAired: { val: new Date(), pt: Pt.DATE, opt: 0 },
    lastAired: { val: new Date(), pt: Pt.DATE, opt: 0 },
};

export type ISeason = ConvertStructToTypes<typeof season>;

export class Season extends TypedData {
    public static refMetaData: YdbTableMetaData;

    constructor(data: ISeason) {
        super(data);
    }
}

// инициализация класса таблицы
Series.initTableDef(databaseName, 'series', series);
Episodes.initTableDef(databaseName, 'episodes', episode);
Season.initTableDef(databaseName, 'season', season);
