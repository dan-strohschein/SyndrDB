import { Statement, IREnvelope } from './generated/types';
export declare function compile(stmt: Statement): string;
export declare function compileAll(env: IREnvelope): string[];
