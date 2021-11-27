import 'reflect-metadata';
import {
  Ydb,
  TypedData,
  YdbTableMetaData,
  ConvertStructToTypes,
} from 'ydb-sdk';
import { databaseName } from './ydb-functions';

const Pt = Ydb.Type.PrimitiveTypeId;

const tdef = {
  id: { val: 0, pt: Pt.UINT64, opt: 'r', pk: true },
  title: { val: 'title', pt: Pt.UTF8, opt: 0 },
  genre_ids: { val: 'json', pt: Pt.JSON, opt: 0 },
  release_date: { val: new Date(), pt: Pt.DATE, opt: 0 },
  adult: { val: false, pt: Pt.BOOL, opt: 0 },
  backdrop_path: { val: 'bp', pt: Pt.UTF8, opt: 0 },
  original_language: { val: 'en', pt: Pt.UTF8, opt: 0 },
  original_title: { val: 'Free Guy', pt: Pt.UTF8, opt: 0 },
  overview: {
    val: 'У сотрудника крупного банка всё идёт по накатанной',
    pt: Pt.UTF8,
    opt: 0,
  },
  popularity: { val: 1, pt: Pt.FLOAT, opt: 0 },
  poster_path: { val: '/qJZ3UeKA', pt: Pt.UTF8, opt: 0 },
  video: { val: false, pt: Pt.BOOL, opt: 0 },
  vote_average: { val: 1, pt: Pt.FLOAT, opt: 0 },
  vote_count: { val: 1, pt: Pt.UINT32, opt: 0 },
};

export type ITdef = ConvertStructToTypes<typeof tdef>;

export class Tdef extends TypedData {
  public static refMetaData: YdbTableMetaData;

  constructor(data: ITdef) {
    super(data);
  }
}

// инициализация класса таблицы
Tdef.initTableDef(Tdef, 'tmdb', databaseName, tdef);
/*
// вывод сгенерированной информации
console.log('Tdef.refMetaData');
console.log(Tdef.refMetaData);


const a1 = new Tdef({ id: 452, title: 'as' });
console.log(a1.createQueryParams());

console.log(Tdef.refMetaData.fieldsDescriptions);

console.log('Tdef.refMetaData');
console.log(Tdef.refMetaData.YQLUpsertSeries);
console.log();

console.log(Tdef.refMetaData.YQLUpsert);


const a = new Tdef({ id: 20, title: 'asd' });
console.log(a);
console.log('---');
process.exit(777);


 */
