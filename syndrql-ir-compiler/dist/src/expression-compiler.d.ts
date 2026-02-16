import { Expr, SelectStmt } from './generated/types';
export declare function compileExpr(e: Expr | undefined): string;
export declare function compileSelectInner(s: SelectStmt): string;
