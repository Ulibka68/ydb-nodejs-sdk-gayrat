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
    episodeId: { val: 0, pt: Pt.UINT64, opt: 'r', pk: true },
    seriesId: { val: 0, pt: Pt.UINT64, opt: 0 },
    seasonId: { val: 0, pt: Pt.UINT64, opt: 0 },
    title: { val: 'title', pt: Pt.UTF8, opt: 0 },
    airDate: { val: new Date(), pt: Pt.DATE, opt: 0 },
};

export type IEpisode = ConvertStructToTypes<typeof episode>;

export class Episode extends TypedData {
    public static refMetaData: YdbTableMetaData;

    constructor(data: IEpisode) {
        super(data);
    }
}

const season = {
    seasonId: { val: 0, pt: Pt.UINT64, opt: 'r', pk: true },
    seriesId: { val: 0, pt: Pt.UINT64, opt: 0 },
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
Series.initTableDef(Series, 'series', databaseName, series);
Episode.initTableDef(Episode, 'episode', databaseName, episode);
Season.initTableDef(Season, 'season', databaseName, season);
