import _ from 'lodash';
import Long from 'long';
import {google, Ydb} from '../proto/bundle';
import 'reflect-metadata';

import Type = Ydb.Type;
import IType = Ydb.IType;
import IStructMember = Ydb.IStructMember;
import IValue = Ydb.IValue;
import IColumn = Ydb.IColumn;
import ITypedValue = Ydb.ITypedValue;
import IResultSet = Ydb.IResultSet;
import NullValue = google.protobuf.NullValue;
import PrimitiveTypeId = Ydb.Type.PrimitiveTypeId;
import { Session, TableDescription, Column } from "./table";
import { Logger} from './logging';
import { withRetries } from './retries';

export const typeMetadataKey = Symbol('type');

export function declareType(type: IType) {
    return Reflect.metadata(typeMetadataKey, type);
}

export function declareTypePrim(typePrim: PrimitiveTypeId) {
    const type: Ydb.IType = { typeId: typePrim };
    return declareType(type);
}
export function declareTypeNull(typePrim: PrimitiveTypeId) {
    const type: Ydb.IType = { optionalType: { item: { typeId: typePrim } } };
    return declareType(type);
}

export const primitiveTypeToValue: Record<number, string> = {
    [Type.PrimitiveTypeId.BOOL]: 'boolValue',
    [Type.PrimitiveTypeId.INT8]: 'int32Value',
    [Type.PrimitiveTypeId.UINT8]: 'uint32Value',
    [Type.PrimitiveTypeId.INT16]: 'int32Value',
    [Type.PrimitiveTypeId.UINT16]: 'uint32Value',
    [Type.PrimitiveTypeId.INT32]: 'int32Value',
    [Type.PrimitiveTypeId.UINT32]: 'uint32Value',
    [Type.PrimitiveTypeId.INT64]: 'int64Value',
    [Type.PrimitiveTypeId.UINT64]: 'uint64Value',
    [Type.PrimitiveTypeId.FLOAT]: 'floatValue',
    [Type.PrimitiveTypeId.DOUBLE]: 'doubleValue',
    [Type.PrimitiveTypeId.STRING]: 'bytesValue',
    [Type.PrimitiveTypeId.UTF8]: 'textValue',
    [Type.PrimitiveTypeId.YSON]: 'bytesValue',
    [Type.PrimitiveTypeId.JSON]: 'textValue',
    [Type.PrimitiveTypeId.UUID]: 'textValue',
    [Type.PrimitiveTypeId.JSON_DOCUMENT]: 'textValue',

    [Type.PrimitiveTypeId.DATE]: 'uint32Value',
    [Type.PrimitiveTypeId.DATETIME]: 'uint32Value',
    [Type.PrimitiveTypeId.TIMESTAMP]: 'uint64Value',
    [Type.PrimitiveTypeId.INTERVAL]: 'uint64Value',
    [Type.PrimitiveTypeId.TZ_DATE]: 'textValue',
    [Type.PrimitiveTypeId.TZ_DATETIME]: 'textValue',
    [Type.PrimitiveTypeId.TZ_TIMESTAMP]: 'textValue',
};

type primitive = boolean | string | number | Date;

export class Primitive {
    static create(type: Ydb.Type.PrimitiveTypeId, value: primitive): ITypedValue {
        return {
            type: {
                typeId: type
            },
            value: {
                [primitiveTypeToValue[type]]: preparePrimitiveValue(type, value)
            }
        };
    }

    static int8(value: number): ITypedValue {
        return Primitive.create(Type.PrimitiveTypeId.INT8, value);
    }

    static uint8(value: number): ITypedValue {
        return Primitive.create(Type.PrimitiveTypeId.UINT8, value);
    }

    static int16(value: number): ITypedValue {
        return Primitive.create(Type.PrimitiveTypeId.INT16, value);
    }

    static uint16(value: number): ITypedValue {
        return Primitive.create(Type.PrimitiveTypeId.UINT16, value);
    }

    static int32(value: number): ITypedValue {
        return Primitive.create(Type.PrimitiveTypeId.INT32, value);
    }

    static uint32(value: number): ITypedValue {
        return Primitive.create(Type.PrimitiveTypeId.UINT32, value);
    }

    static int64(value: number): ITypedValue {
        return Primitive.create(Type.PrimitiveTypeId.INT64, value);
    }

    static uint64(value: number): ITypedValue {
        return Primitive.create(Type.PrimitiveTypeId.UINT64, value);
    }

    static float(value: number): ITypedValue {
        return Primitive.create(Type.PrimitiveTypeId.FLOAT, value);
    }

    static double(value: number): ITypedValue {
        return Primitive.create(Type.PrimitiveTypeId.DOUBLE, value);
    }

    static string(value: string): ITypedValue {
        return Primitive.create(Type.PrimitiveTypeId.STRING, value);
    }

    static utf8(value: string): ITypedValue {
        return Primitive.create(Type.PrimitiveTypeId.UTF8, value);
    }

    static yson(value: string): ITypedValue {
        return Primitive.create(Type.PrimitiveTypeId.YSON, value);
    }

    static json(value: string): ITypedValue {
        return Primitive.create(Type.PrimitiveTypeId.JSON, value);
    }

    static uuid(value: string): ITypedValue {
        return Primitive.create(Type.PrimitiveTypeId.UUID, value);
    }

    static jsonDocument(value: string): ITypedValue {
        return Primitive.create(Type.PrimitiveTypeId.JSON_DOCUMENT, value);
    }

    static date(value: Date): ITypedValue {
        return Primitive.create(Type.PrimitiveTypeId.DATE, value);
    }

    static datetime(value: Date): ITypedValue {
        return Primitive.create(Type.PrimitiveTypeId.DATETIME, value);
    }

    static timestamp(value: Date): ITypedValue {
        return Primitive.create(Type.PrimitiveTypeId.TIMESTAMP, value);
    }

    static tzDate(value: Date): ITypedValue {
        return Primitive.create(Type.PrimitiveTypeId.TZ_DATE, value);
    }

    static tzDatetime(value: Date): ITypedValue {
        return Primitive.create(Type.PrimitiveTypeId.TZ_DATETIME, value);
    }

    static tzTimestamp(value: Date): ITypedValue {
        return Primitive.create(Type.PrimitiveTypeId.TZ_TIMESTAMP, value);
    }
}

const parseLong = (input: string|number): Long|number => {
   const long = typeof input === 'string' ? Long.fromString(input) : Long.fromNumber(input);
   return long.high ? long : long.low;
};

const valueToNativeConverters: Record<string, (input: string|number) => any> = {
    'boolValue': (input) => Boolean(input),
    'int32Value': (input) => Number(input),
    'uint32Value': (input) => Number(input),
    'int64Value': (input) => parseLong(input),
    'uint64Value': (input) => parseLong(input),
    'floatValue': (input) => Number(input),
    'doubleValue': (input) => Number(input),
    'bytesValue': (input) => Buffer.from(input as string, 'base64').toString(),
    'textValue': (input) => input,
    'nullFlagValue': () => null,
};
function convertPrimitiveValueToNative(type: IType, value: IValue) {
    let label, input;
    for ([label, input] of Object.entries(value)) {
        if (label !== 'items' && label !== 'pairs') {
            break;
        }
    }
    if (!label) {
        throw new Error(`Expected a primitive value, got ${value} instead!`);
    }

    let typeId: PrimitiveTypeId | null = null;
    if (type.optionalType) {
        const innerType = type.optionalType.item;
        if (label === 'nullFlagValue') {
            return null;
        } else if (innerType && innerType.typeId) {
            typeId = innerType.typeId;
        }
    } else if (type.typeId) {
        typeId = type.typeId;
    }
    if (typeId === null) {
        throw new Error(`Got empty typeId, type is ${JSON.stringify(type)}, value is ${JSON.stringify(value)}.`);
    }
    return objectFromValue(typeId, valueToNativeConverters[label](input));
}

function objectFromValue(typeId: PrimitiveTypeId, value: unknown) {
    switch (typeId) {
        case PrimitiveTypeId.DATE:
            return new Date((value as number) * 3600 * 1000 * 24);
        case PrimitiveTypeId.DATETIME:
            return new Date((value as number) * 1000);
        case PrimitiveTypeId.TIMESTAMP:
            return new Date((value as number) / 1000);
        case PrimitiveTypeId.TZ_DATE:
        case PrimitiveTypeId.TZ_DATETIME:
        case PrimitiveTypeId.TZ_TIMESTAMP:
            return new Date(value as string);
        default:
            return value;
    }
}

function preparePrimitiveValue(typeId: PrimitiveTypeId, value: any) {
    switch (typeId) {
        case PrimitiveTypeId.DATE:
            return Number(value) / 3600 / 1000 / 24;
        case PrimitiveTypeId.DATETIME:
            return Number(value) / 1000;
        case PrimitiveTypeId.TIMESTAMP:
            return Number(value) * 1000;
        case PrimitiveTypeId.TZ_DATE:
            return (value as Date).toDateString();
        case PrimitiveTypeId.TZ_DATETIME:
        case PrimitiveTypeId.TZ_TIMESTAMP:
            return (value as Date).toISOString();
        default:
            return value;
    }
}

function typeToValue(type: IType | null | undefined, value: any): IValue {
    if (!type) {
        if (value) {
            throw new Error(`Got no type while the value is ${value}`);
        } else {
            throw new Error('Both type and value are empty');
        }
    } else if (type.typeId) {
        const valueLabel = primitiveTypeToValue[type.typeId];
        if (valueLabel) {
            return {[valueLabel]: preparePrimitiveValue(type.typeId, value)};
        } else {
            throw new Error(`Unknown PrimitiveTypeId: ${type.typeId}`);
        }
    } else if (type.decimalType) {
        const numericValue = BigInt(value);
        const low = numericValue & BigInt('0xffffffffffffffff');
        const hi = numericValue >> BigInt('64');
        return {
            low_128: Long.fromString(low.toString()),
            high_128: Long.fromString(hi.toString())
        }
    } else if (type.optionalType) {
        const innerType = type.optionalType.item;
        if (value !== undefined && value !== null) {
            return typeToValue(innerType, value);
        } else {
            return {
                nullFlagValue: NullValue.NULL_VALUE
            };
        }
    } else if (type.listType) {
        const listType = type.listType;
        return {
            items: _.map(value, (item) => typeToValue(listType.item, item))
        };
    } else if (type.tupleType) {
        const elements = type.tupleType.elements as IType[];
        return {
            items: _.map(value, (item, index: number) => typeToValue(elements[index], item))
        };
    } else if (type.structType) {
        const members = type.structType.members as IStructMember[];
        return {
            items: _.map(value, (item, index: number) => {
                const type = members[index].type;
                return typeToValue(type, item);
            })
        };
    } else if (type.dictType) {
        const keyType = type.dictType.key as IType;
        const payloadType = type.dictType.payload as IType;
        return {
            pairs: _.map(_.entries(value), ([key, value]) => ({
                key: typeToValue(keyType, key),
                payload: typeToValue(payloadType, value)
            }))
        }
    } else if (type.variantType) {
        let variantIndex = -1;
        if (type.variantType.tupleItems) {
            const elements = type.variantType.tupleItems.elements as IType[];
            return {
                items: _.map(value, (item, index: number) => {
                    if (item) {
                        variantIndex = index;
                        return typeToValue(elements[index], item);
                    }
                    return item;
                }),
                variantIndex
            }
        } else if (type.variantType.structItems) {
            const members = type.variantType.structItems.members as IStructMember[];
            return {
                items: _.map(value, (item, index: number)=> {
                    if (item) {
                        variantIndex = index;
                        const type = members[index].type;
                        return typeToValue(type, item);
                    }
                    return item;
                }),
                variantIndex
            }
        }
        throw new Error('Either tupleItems or structItems should be present in VariantType!');
    } else {
        throw new Error(`Unknown type ${type}`);
    }
}

type StringFunction = (name?: string) => string;
export interface NamesConversion {
    ydbToJs: StringFunction;
    jsToYdb: StringFunction;
}

export interface TypedDataOptions {
    namesConversion?: NamesConversion;
}

function assertUnreachable(_c: never): never {
    throw new Error('Should not get here!');
}
function getNameConverter(options: TypedDataOptions, direction: keyof NamesConversion): StringFunction {
    const converter = options.namesConversion?.[direction];
    if (converter) {
        return converter;
    } else { // defaults
        switch (direction) {
            case 'jsToYdb': return _.snakeCase;
            case 'ydbToJs': return _.camelCase;
            default: return assertUnreachable(direction);
        }
    }
}

export function withTypeOptions(options: TypedDataOptions) {
    return function<T extends Function>(constructor: T): T & {__options: TypedDataOptions} {
        return _.merge(constructor, {__options: options});
    }
}

export const snakeToCamelCaseConversion: NamesConversion = {
    jsToYdb: _.snakeCase,
    ydbToJs: _.camelCase,
};
export const identityConversion: NamesConversion = {
    jsToYdb: _.identity,
    ydbToJs: _.identity,
}


export type NonFunctionKeys<T extends object> = {
    [K in keyof T]-?: T[K] extends Function ? never : K;
}[keyof T];

export type ITableFromClass<T extends object> = { [K in NonFunctionKeys<T>]: T[K] };


export type NonNeverKeys<T extends object> = {
    [K in keyof T]-?: T[K] extends never ? never : K;
}[keyof T];

// тест
// type T1 = {
//   a: number;
//   b: never;
//   c: string;
// };
// type O2 = NonNeverKeys<T1>;
// выход
// type O2_r = 'a' | 'c';

export type NonUndefinedKeys<T extends object> = {
    [K in keyof T]-?: T[K] extends undefined ? never : K;
}[keyof T];

export type RemoveNever<T extends object> = { [K in NonNeverKeys<T>]: T[K] };

// тест
// type O3 = RemoveNever<T1>;
// выход
// type O3_r = {
//   a: number;
//   c: string;
// };

export type OptionalUndefined<T extends object> = {
    [K in NonUndefinedKeys<T>]+?: T[K];
};

export type RemoveIntersection<T> = { [K in keyof T]: T[K] };

type RequiredFields<B extends Record<string, FieldsDefinition>> = RemoveNever<{
    [K in keyof B]: B[K]['opt'] extends string ? B[K]['val'] : never;
}>;

type OptionalFields<B extends Record<string, FieldsDefinition>> =
    OptionalUndefined<{
        [K in keyof B]: B[K]['opt'] extends number ? B[K]['val'] : never;
    }>;

/*
  На вход получает тип , образуемый из объекта JS

  const tdef = {
    id: { val: 0, pt: Pt.UINT64, opt: 'r' },
    title: { val: 'title', pt: Pt.UTF8, opt: 0 },
    genre_ids: { val: 'json', pt: Pt.JSON, opt: 0 },
    release_date: { val: new Date(), pt: Pt.DATE, opt: 0 },
  };

  type ITdef = ConvertStructToTypes<typeof tdef>;

  На выходе получаем :
  {
    id : number;
    title? : string; // опциональный параметр
    genre_ids? : string;
    release_date? : Date;
  }

  Новые записи можно создавать передавая только обязательные параметры
  const rec = new Tdef({ id: 25 });

 */
export type ConvertStructToTypes<T extends Record<string, FieldsDefinition>> =
    RemoveIntersection<RequiredFields<T> & OptionalFields<T>>;


export interface TypedDataFieldDescription {
 name: string;
 typeId: number;
 optional: boolean;
 typeName: string;
 primaryKey?: boolean;
}

export interface FieldsDefinition {
    val: any;
    pt: Ydb.Type.PrimitiveTypeId;
    opt: string | number;
    pk?: boolean; // primary key
}

export type TableDefinition = Record<string, FieldsDefinition>;

export class YdbTableMetaData {
        public tableName: string='';
        public fieldsDescriptions: Array<TypedDataFieldDescription>=[];
        public   YQLUpsert:string='';
        public   YQLUpsertSeries:string='';
        public   YQLCreateTable:string='';
        public tableDef: TableDefinition={};
}

export class TypedData {
    [property: string]: any;
    static __options: TypedDataOptions = {};

    public static refMetaData : YdbTableMetaData;

    constructor(data: Record<string, any>) {
        // _.assign(this, data);
        // если в наследующем классе написано public a : string = undefined - то lodash присваивание не производит
        Reflect.ownKeys(data).forEach((key) => {
            this[key as string] = data[key as string];
        });
       // this.expandFields();

        this.generateMetadata();
    }

    getType(propertyKey: string): IType {
        const typeMeta = Reflect.getMetadata(typeMetadataKey, this, propertyKey);
        if (!typeMeta) {
            throw new Error(`Property ${propertyKey} should be decorated with @declareType!`);
        }
        return typeMeta;
    }

    getValue(propertyKey: string): IValue {
        const type = this.getType(propertyKey);
        return typeToValue(type, this[propertyKey]);
    }

    getTypedValue(propertyKey: string): ITypedValue {
        return {
            type: this.getType(propertyKey),
            value: this.getValue(propertyKey)
        };
    }

    get typedProperties(): string[] {
        return _.filter(Reflect.ownKeys(this), (key) => (
            typeof key === 'string' && Reflect.hasMetadata(typeMetadataKey, this, key)
        )) as string[];
    }

    getRowType() {
        const cls = this.constructor as typeof TypedData;
        const converter = getNameConverter(cls.__options, 'jsToYdb');
        return {
            structType: {
                members: _.map(this.typedProperties, (propertyKey) => ({
                    name: converter(propertyKey),
                    type: this.getType(propertyKey)
                }))
            }
        };
    }


    getRowValue() {
        return {
            items: _.map(this.typedProperties, (propertyKey: string) => {
                return this.getValue(propertyKey)
            })
        }
    }

    static createNativeObjects(resultSet: IResultSet): TypedData[] {
        const {rows, columns} = resultSet;
        if (!columns) {
            return [];
        }
        const converter = getNameConverter(this.__options, 'ydbToJs');
        return _.map(rows, (row) => {
            const obj = _.reduce(row.items, (acc: Record<string, any>, value, index) => {
                const column = columns[index] as IColumn;
                if (column.name && column.type) {
                    acc[converter(column.name)] = convertPrimitiveValueToNative(column.type, value);
                }
                return acc;
            }, {});
            return new this(obj);
        })
    }

    static asTypedCollection(collection: TypedData[]) {
        return {
            type: {
                listType: {
                    item: collection[0].getRowType()
                }
            },
            value: {
                items: collection.map((item) => item.getRowValue())
            }
        }
    }

    expandFields() {
        const refMeta : YdbTableMetaData = (this.constructor as typeof TypedData).refMetaData;
        if (! refMeta ) return; // если описание таблицы идет старым способом
        Reflect.ownKeys(refMeta.tableDef).forEach(key => {
            key=key as string;
            if (! this[key]) {
                this[key]=null;
            }
        })
    }

    createQueryParams() {
        const resObj:Record<string,any>={};
        // @ts-ignore   доступ к static свойству derived класса
        const refMeta : YdbTableMetaData = this.constructor.refMetaData;

        /*
        // выдаст все параметры
        refMeta.fieldsDescriptions.forEach((fld)=>{
            resObj['$'+fld.name]=this.getTypedValue(fld.name)
        })*/

        Reflect.ownKeys(this).forEach(key   => {
            key=key.toString();
            resObj['$'+key]=this.getTypedValue(key)
        })


        return resObj;
    }

    generateYQLUpsert( databaseName: string) {
        let rst = `PRAGMA TablePathPrefix("${databaseName}");`;
        let rst_series = `PRAGMA TablePathPrefix("${databaseName}");\nDECLARE $seriesData AS List<Struct<`;

        const refMeta : YdbTableMetaData=(this.constructor as typeof TypedData).refMetaData;

        const tpo = this.getRowType().structType.members.map((itm) => {
            const res = { name: itm.name, typeId: 0, optional: false, typeName: '' };
            if (itm.type.typeId) {
                res.typeId = itm.type.typeId;
            } else {
                res.typeId = itm.type!.optionalType!.item!.typeId as number;
                res.optional = true;
            }

            res.typeName = primitiveTypeIdToName[res.typeId];
            return res;
        });
        refMeta.fieldsDescriptions=tpo;

        tpo.forEach((itm) => {
            rst += `\nDECLARE $${itm.name} as ${itm.typeName}${
                itm.optional ? '?' : ''
            };`;
            rst_series += `\n   ${itm.name} : ${itm.typeName}${itm.optional ? '?' : ''},`;
        });
        rst_series = rst_series.substring(0,rst_series.length-1);
        rst_series += `>>;\n\nUPSERT INTO ${refMeta.tableName}\nSELECT *`;

        rst += `\n\nUPSERT INTO ${refMeta.tableName} (`;
        tpo.forEach((itm) => {
            rst += `\n   ${itm.name},`;
            // rst_series += `\n   ${itm.name},`;
        });

        rst = rst.substring(0, rst.length - 1);
        // rst_series = rst_series.substring(0,rst_series.length-1);
        rst_series += '\nFROM AS_TABLE($seriesData);'

        rst += '\n)VALUES (';
        tpo.forEach((itm) => {
            rst += `\n   $${itm.name},`;
        });
        rst = rst.substring(0, rst.length - 1);
        rst += '\n);';

        refMeta.YQLUpsert=rst;
        refMeta.YQLUpsertSeries=rst_series;
    } // generateYQLUpsert

    static generateInitialData(tableDef: TableDefinition) {
        const resultObj: any = {};
        Reflect.ownKeys(tableDef).forEach((key) => {
            key = key as string;
            resultObj[key] = tableDef[key].val;
        });
        return resultObj;
    } // generateInitialData

    generateMetadata() {
        // @ts-ignore
        if (!this.constructor.refMetaData) return;
        // @ts-ignore
        const tableDef: TableDefinition=this.constructor.refMetaData.tableDef;
        Reflect.ownKeys(tableDef).forEach((key) => {
            let metadataValue: any = {};
            key = key as string;

            if (tableDef[key].opt === 'r') {
                metadataValue = { typeId: tableDef[key].pt };
            } else
                metadataValue = {
                    optionalType: { item: { typeId: tableDef[key].pt } },
                };

            Reflect.defineMetadata(typeMetadataKey, metadataValue, this, key);
        });
    } // generateMetadata

    static generateYQLcreateTable(
        databaseName: string
    ) {
        let rst = `PRAGMA TablePathPrefix("${databaseName}");\n`;
        let rst_primary = `\n    PRIMARY KEY (`;
        let first_primary = true;

        rst += `CREATE TABLE ${this.refMetaData.tableName} (`;
        Reflect.ownKeys(this.refMetaData.tableDef).forEach((key) => {
            key = key as string;
            rst += `\n    ${key} ${primitiveTypeIdToName[this.refMetaData.tableDef[key].pt]},`;
            if (this.refMetaData.tableDef[key].pk) {
                if (!first_primary) {
                    rst_primary += ',';
                }
                first_primary = false;
                rst_primary += key;
            }
        });
        rst_primary += ')';
        rst += rst_primary + '\n)';
        // return rst;
        this.refMetaData.YQLCreateTable = rst;
    } // generateYQLcreateTable

    static setPrimaryKeyField() {
        Reflect.ownKeys(this.refMetaData.tableDef).forEach((key) => {
            key = key as string;
            if (this.refMetaData.tableDef[key].pk) {
                const idx= this.refMetaData.fieldsDescriptions.findIndex( (element)=> (element.name === key));
                if (idx >=0) this.refMetaData.fieldsDescriptions[idx].primaryKey=true;
            }
        });
    }

    static initTableDef(ctor : {new (data: any):any},tableName: string,databaseName : string, tdef : TableDefinition) {
        this.refMetaData = new YdbTableMetaData();
        this.refMetaData.tableName = tableName;
        this.refMetaData.tableDef = tdef;

        const rec = new ctor(TypedData.generateInitialData(tdef ));
        rec.generateYQLUpsert(databaseName);
        this.generateYQLcreateTable(databaseName);
        this.setPrimaryKeyField();
    }

    async upsertToDB(session: Session, logger: Logger ) {

        const YQLUpsert =  (this.constructor as typeof TypedData).refMetaData.YQLUpsert;
        const thisRecord =this;

        async function fillTable() {
            logger.info('Inserting data to tables, preparing query...');
            // @ts-ignore
            let preparedQuery: Ydb.Table.PrepareQueryResult;
            try {
                preparedQuery = await session.prepareQuery( YQLUpsert );
                logger.info('Query has been prepared, executing...');
                await session.executeQuery(preparedQuery, thisRecord.createQueryParams());
            } catch (err) {
                if (err instanceof Error) {
                    console.error(err.message);
                    process.exit(55);
                }
            }

        }

        await withRetries(fillTable);
    }

    static async upsertSeriesToDB(session: Session, logger: Logger, dataArray : Array<TypedData> ) {

        const YQLUpsert =  this.refMetaData.YQLUpsertSeries;
       /* const YQLUpsert =`PRAGMA TablePathPrefix("${databaseName}");
        Declare $seriesData  as List<Struct<id: Uint64, title : Utf8?, adult : BOOL?>>;
        UPSERT INTO ${this.refMetaData.tableName} SELECT  * FROM AS_TABLE($seriesData);`*/


        async function fillTable() {
            logger.info('Inserting data to tables, preparing query...');
            try {
                const preparedQuery = await session.prepareQuery( YQLUpsert );
                logger.info('Query has been prepared, executing...');
                await session.executeQuery(preparedQuery,{'$seriesData': TypedData.asTypedCollection(dataArray)})
                    // thisRecord.createQueryParams());
            } catch (err) {
                if (err instanceof Error) {
                    console.error(err.message);
                    process.exit(55);
                }
            }

        }

        await withRetries(fillTable);
    }

    /*
    Так не работает
    static async createDBTable(session: Session, logger: Logger ) {

        const YQLCreateTable =  this.refMetaData.YQLCreateTable;

        async function createTable() {
            logger.info('Creating table...');
                await session.executeQuery(YQLCreateTable);
        }

        await withRetries(createTable);
    }*/

    static async createDBTable(session: Session, logger: Logger )  {
        logger.info('Creating table... ' + this.refMetaData.tableName);
        const columns : Array<Column>=[];
        const primaryKeys : Array<string>=[];
        this.refMetaData.fieldsDescriptions.forEach(fld=>{
            const type ={optionalType: {item: {typeId: fld.typeId}}};
            /*

            // Error : Only optional type for columns supported
            if (fld.optional) {
                type={optionalType: {item: {typeId: fld.typeId}}};
            } else {
                type= {typeId: fld.typeId};
            }
             */

            const column = new Column(
                fld.name,
                Ydb.Type.create(type)
            );
            columns.push(column);
             if (fld.primaryKey) {
                 primaryKeys.push(fld.name)
             }
        })

        await session.createTable(
            this.refMetaData.tableName,
            new TableDescription()
                .withColumns(...columns)
                .withPrimaryKeys(...primaryKeys)
        );
    }

    static async dropDBTable(session: Session, logger: Logger )  {
        logger.info('Drop table... ' + this.refMetaData.tableName);
        await session.dropTable(this.refMetaData.tableName);
    }

}

export const primitiveTypeIdToName : Record<string, string> = {};


function initPrimitiveTypeIdToName() {
    for (const itm of Object.entries(PrimitiveTypeId) ) {
        // @ts-ignore
        primitiveTypeIdToName[itm[1]] = itm[0];
    }
}

// Side Effect
initPrimitiveTypeIdToName();
