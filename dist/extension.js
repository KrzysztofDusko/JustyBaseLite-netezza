"use strict";var Tt=Object.create;var Ce=Object.defineProperty;var bt=Object.getOwnPropertyDescriptor;var Ct=Object.getOwnPropertyNames;var St=Object.getPrototypeOf,At=Object.prototype.hasOwnProperty;var J=(l,e)=>()=>(l&&(e=l(l=0)),e);var Q=(l,e)=>{for(var t in e)Ce(l,t,{get:e[t],enumerable:!0})},Qe=(l,e,t,s)=>{if(e&&typeof e=="object"||typeof e=="function")for(let n of Ct(e))!At.call(l,n)&&n!==t&&Ce(l,n,{get:()=>e[n],enumerable:!(s=bt(e,n))||s.enumerable});return l};var $=(l,e,t)=>(t=l!=null?Tt(St(l)):{},Qe(e||!l||!l.__esModule?Ce(t,"default",{value:l,enumerable:!0}):t,l)),Rt=l=>Qe(Ce({},"__esModule",{value:!0}),l);var oe,_e=J(()=>{"use strict";oe=class l{constructor(e){this.context=e;this._connectionDetails=null;this._persistentConnection=null;this._keepConnectionOpen=!1;this._lastConnectionString=null}static{this.SERVICE_NAME="netezza-vscode"}async saveConnection(e){this._connectionDetails=e,await this.context.secrets.store(l.SERVICE_NAME,JSON.stringify(e))}async getConnection(){if(this._connectionDetails)return this._connectionDetails;let e=await this.context.secrets.get(l.SERVICE_NAME);return e?(this._connectionDetails=JSON.parse(e),this._connectionDetails):null}async getConnectionString(){let e=await this.getConnection();return e?`DRIVER={NetezzaSQL};SERVER=${e.host};PORT=${e.port};DATABASE=${e.database};UID=${e.user};PWD=${e.password};`:null}setKeepConnectionOpen(e){this._keepConnectionOpen=e,e||this.closePersistentConnection()}getKeepConnectionOpen(){return this._keepConnectionOpen}async getPersistentConnection(){let e=await this.getConnectionString();if(!e)throw new Error("Connection not configured. Please connect via Netezza: Connect...");if(this._lastConnectionString!==e||!this._persistentConnection){this.closePersistentConnection();try{let t=require("odbc");this._persistentConnection=await t.connect({connectionString:e,fetchArray:!0}),this._lastConnectionString=e}catch(t){throw this._persistentConnection=null,this._lastConnectionString=null,t}}return this._persistentConnection}closePersistentConnection(){if(this._persistentConnection){try{this._persistentConnection.close()}catch(e){console.error("Error closing persistent connection:",e)}this._persistentConnection=null,this._lastConnectionString=null}}}});var Ye={};Q(Ye,{QueryHistoryManager:()=>q});var re,Se,q,Ae=J(()=>{"use strict";re=$(require("fs")),Se=$(require("path")),q=class l{constructor(e){this.context=e;this.cache=[];this.initialized=!1;this.initialize()}static{this.STORAGE_KEY="queryHistory"}static{this.MAX_ENTRIES=5e4}static{this.CLEANUP_KEEP=4e4}static{this.STORAGE_VERSION=1}async initialize(){try{let e=this.context.globalState.get(l.STORAGE_KEY);e&&e.entries?(this.cache=e.entries,console.log(`\u2705 Loaded ${this.cache.length} entries from VS Code storage`)):await this.migrateFromLegacyStorage(),this.initialized=!0}catch(e){console.error("\u274C Error initializing query history:",e),this.cache=[],this.initialized=!0}}async migrateFromLegacyStorage(){try{let e=this.context.globalStorageUri.fsPath,t=Se.join(e,"query-history.db");re.existsSync(t)&&(console.log("\u26A0\uFE0F SQLite database found but migration not implemented"),console.log("\u{1F4A1} Consider manually exporting data before switching"));let s=Se.join(e,"query-history.json");if(re.existsSync(s)){let i=re.readFileSync(s,"utf8");if(i.trim()){let o=JSON.parse(i);this.cache=o,await this.saveToStorage(),console.log(`\u2705 Migrated ${o.length} entries from JSON`)}}let n=Se.join(e,"query-history-archive.json");if(re.existsSync(n)){let i=re.readFileSync(n,"utf8");if(i.trim()){let o=JSON.parse(i);this.cache.push(...o),await this.saveToStorage(),console.log(`\u2705 Migrated archive with ${o.length} entries`)}}}catch(e){console.error("Error during migration:",e)}}async saveToStorage(){try{let e={entries:this.cache,version:l.STORAGE_VERSION};await this.context.globalState.update(l.STORAGE_KEY,e)}catch(e){console.error("Error saving to storage:",e)}}async addEntry(e,t,s,n,i,o){try{this.initialized||await this.initialize();let a=`${Date.now()}-${Math.random().toString(36).substring(2,9)}`,r=Date.now(),p={id:a,host:e,database:t,schema:s,query:n.trim(),timestamp:r,is_favorite:!1,tags:i||"",description:o||""};this.cache.unshift(p),this.cache.length>l.MAX_ENTRIES&&(this.cache=this.cache.slice(0,l.CLEANUP_KEEP),console.log(`Cleaned up old entries, keeping ${l.CLEANUP_KEEP} newest`)),await this.saveToStorage()}catch(a){console.error("Error adding query to history:",a)}}async getHistory(){return this.initialized||await this.initialize(),[...this.cache]}async deleteEntry(e){try{this.cache=this.cache.filter(t=>t.id!==e),await this.saveToStorage()}catch(t){console.error("Error deleting entry:",t)}}async clearHistory(){try{this.cache=[],await this.saveToStorage(),console.log("All query history cleared")}catch(e){console.error("Error clearing history:",e)}}async getStats(){try{let e=this.cache.length,t=JSON.stringify(this.cache).length,s=parseFloat((t/(1024*1024)).toFixed(2));return{activeEntries:e,archivedEntries:0,totalEntries:e,activeFileSizeMB:s,archiveFileSizeMB:0,totalFileSizeMB:s}}catch(e){return console.error("Error getting stats:",e),{activeEntries:0,archivedEntries:0,totalEntries:0,activeFileSizeMB:0,archiveFileSizeMB:0,totalFileSizeMB:0}}}async toggleFavorite(e){try{let t=this.cache.find(s=>s.id===e);t&&(t.is_favorite=!t.is_favorite,await this.saveToStorage())}catch(t){console.error("Error toggling favorite:",t)}}async updateEntry(e,t,s){try{let n=this.cache.find(i=>i.id===e);n&&(t!==void 0&&(n.tags=t),s!==void 0&&(n.description=s),await this.saveToStorage())}catch(n){console.error("Error updating entry:",n)}}async getFavorites(){return this.initialized||await this.initialize(),this.cache.filter(e=>e.is_favorite)}async getByTag(e){return this.initialized||await this.initialize(),this.cache.filter(t=>t.tags?.toLowerCase().includes(e.toLowerCase()))}async getAllTags(){this.initialized||await this.initialize();let e=new Set;return this.cache.forEach(t=>{t.tags&&t.tags.split(",").forEach(n=>{let i=n.trim();i&&e.add(i)})}),Array.from(e).sort()}async searchAll(e){this.initialized||await this.initialize();let t=e.toLowerCase();return this.cache.filter(s=>s.query.toLowerCase().includes(t)||s.host.toLowerCase().includes(t)||s.database.toLowerCase().includes(t)||s.schema.toLowerCase().includes(t)||s.tags?.toLowerCase().includes(t)||s.description?.toLowerCase().includes(t))}async getFilteredHistory(e,t,s,n){this.initialized||await this.initialize();let i=this.cache.filter(o=>!(e&&o.host!==e||t&&o.database!==t||s&&o.schema!==s));return n&&(i=i.slice(0,n)),i}async getArchivedHistory(){return[]}async clearArchive(){}close(){console.log("Query history manager closed")}}});var st={};Q(st,{runQueriesSequentially:()=>Be,runQuery:()=>L,runQueryRaw:()=>nt});function Ge(l){let e=l.match(/SERVER=([^;]+)/i),t=l.match(/DATABASE=([^;]+)/i);return{host:e?e[1]:"unknown",database:t?t[1]:"unknown"}}function Ke(l){let e=new Set;if(!l)return e;for(let t of l.matchAll(/\$\{([A-Za-z0-9_]+)\}/g))t[1]&&e.add(t[1]);return e}function Ze(l){if(!l)return{sql:"",setValues:{}};let e=l.split(/\r?\n/),t=[],s={};for(let n of e){let i=n.match(/^\s*@SET\s+([A-Za-z0-9_]+)\s*=\s*(.+)$/i);if(i){let o=i[2].trim();o.endsWith(";")&&(o=o.slice(0,-1).trim());let a=o.match(/^'(.*)'$/s)||o.match(/^"(.*)"$/s);a&&(o=a[1]),s[i[1]]=o}else t.push(n)}return{sql:t.join(`
`),setValues:s}}async function et(l,e,t){let s={};if(l.size===0)return s;if(e){let i=Array.from(l).filter(o=>!(t&&t[o]!==void 0));if(i.length>0)throw new Error("Query contains variables but silent mode is enabled; cannot prompt for values. Missing: "+i.join(", "));for(let o of l)s[o]=t[o];return s}let n=[];for(let i of l)t&&t[i]!==void 0?s[i]=t[i]:n.push(i);for(let i of n){let o=await Re.window.showInputBox({prompt:`Enter value for ${i}`,placeHolder:"",value:t&&t[i]?t[i]:void 0,ignoreFocusOut:!0});if(o===void 0)throw new Error("Variable input cancelled by user");s[i]=o}return s}function tt(l,e){return l.replace(/\$\{([A-Za-z0-9_]+)\}/g,(t,s)=>e[s]??"")}async function nt(l,e,t=!1,s){if(!pe)throw new Error("odbc package not installed. Please run: npm install odbc");let n=s||new oe(l),i=n.getKeepConnectionOpen(),o;t||(o=Re.window.createOutputChannel("Netezza SQL"),o.show(!0),o.appendLine("Executing query..."));try{let a=Ze(e),r=a.sql,p=a.setValues,u=Ke(r);if(u.size>0){let y=await et(u,t,p);r=tt(r,y)}let m,f=!0,g;if(i){m=await n.getPersistentConnection(),f=!1;let y=await n.getConnectionString();if(!y)throw new Error("Connection not configured. Please connect via Netezza: Connect...");g=y}else{let y=await n.getConnectionString();if(!y)throw new Error("Connection not configured. Please connect via Netezza: Connect...");g=y,m=await pe.connect({connectionString:g,fetchArray:!0})}try{let y=await m.query(r),S="unknown";try{let N=await m.query("SELECT CURRENT_SCHEMA");N&&N.length>0&&(S=N[0].CURRENT_SCHEMA||"unknown")}catch(N){console.debug("Could not retrieve current schema:",N)}let R=Ge(g);if(new q(l).addEntry(R.host,R.database,S,e).catch(N=>{console.error("Failed to log query to history:",N)}),y&&Array.isArray(y)){let N=y.columns?y.columns.map(M=>({name:M.name,type:M.dataType})):[];return o&&o.appendLine("Query completed."),{columns:N,data:y,rowsAffected:y.count}}else return o&&o.appendLine("Query executed successfully (no results)."),{columns:[],data:[],rowsAffected:y?.count,message:"Query executed successfully (no results)."}}finally{f&&await m.close()}}catch(a){let r=ze(a);throw o&&o.appendLine(r),new Error(r)}}async function L(l,e,t=!1){try{let s=await nt(l,e,t);if(s.data&&s.data.length>0){let n=s.data.map(o=>{let a={};return s.columns.forEach((r,p)=>{a[r.name]=o[p]}),a});return JSON.stringify(n,(o,a)=>typeof a=="bigint"?a>=Number.MIN_SAFE_INTEGER&&a<=Number.MAX_SAFE_INTEGER?Number(a):a.toString():a,2)}else if(s.message)return s.message;return}catch(s){throw s}}async function Be(l,e,t){if(!pe)throw new Error("odbc package not installed. Please run: npm install odbc");let s=t||new oe(l),n=s.getKeepConnectionOpen(),i=Re.window.createOutputChannel("Netezza SQL");i.show(!0),i.appendLine(`Executing ${e.length} queries sequentially...`);let o=[];try{let a,r=!0,p;if(n){a=await s.getPersistentConnection(),r=!1;let u=await s.getConnectionString();if(!u)throw new Error("Connection not configured. Please connect via Netezza: Connect...");p=u}else{let u=await s.getConnectionString();if(!u)throw new Error("Connection not configured. Please connect via Netezza: Connect...");p=u,a=await pe.connect({connectionString:p,fetchArray:!0})}try{let u="unknown";try{let g=await a.query("SELECT CURRENT_SCHEMA");g&&g.length>0&&(u=g[0].CURRENT_SCHEMA||"unknown")}catch(g){console.debug("Could not retrieve current schema:",g)}let m=Ge(p),f=new q(l);for(let g=0;g<e.length;g++){let y=e[g];i.appendLine(`Executing query ${g+1}/${e.length}...`);try{let S=Ze(y),R=S.sql,D=S.setValues,N=Ke(R);if(N.size>0){let k=await et(N,!1,D);R=tt(R,k)}let M=await a.query(R);if(f.addEntry(m.host,m.database,u,y).catch(k=>{console.error("Failed to log query to history:",k)}),M&&Array.isArray(M)){let k=M.columns?M.columns.map(te=>({name:te.name,type:te.dataType})):[];o.push({columns:k,data:M,rowsAffected:M.count})}else o.push({columns:[],data:[],rowsAffected:M?.count,message:"Query executed successfully"})}catch(S){let R=ze(S);throw i.appendLine(`Error in query ${g+1}: ${R}`),new Error(R)}}i.appendLine("All queries completed.")}finally{r&&await a.close()}}catch(a){let r=ze(a);throw i.appendLine(r),new Error(r)}return o}function ze(l){return l.odbcErrors&&Array.isArray(l.odbcErrors)&&l.odbcErrors.length>0?l.odbcErrors.map(e=>`[ODBC Error] State: ${e.state}, Native Code: ${e.code}
Message: ${e.message}`).join(`

`):`Error: ${l.message||l}`}var Re,pe,ae=J(()=>{"use strict";Re=$(require("vscode"));_e();Ae();try{pe=require("odbc")}catch{console.error("odbc package not installed. Run: npm install odbc")}});var ot={};Q(ot,{SchemaItem:()=>Y,SchemaProvider:()=>ge});var O,ge,Y,Ue=J(()=>{"use strict";O=$(require("vscode"));ae();ge=class{constructor(e,t){this.context=e;this.connectionManager=t;this._onDidChangeTreeData=new O.EventEmitter;this.onDidChangeTreeData=this._onDidChangeTreeData.event}refresh(){this._onDidChangeTreeData.fire()}getTreeItem(e){return e}getParent(e){if(e.contextValue!=="database"){if(e.contextValue==="typeGroup")return new Y(e.dbName,O.TreeItemCollapsibleState.Collapsed,"database",e.dbName);if(e.contextValue.startsWith("netezza:"))return new Y(e.objType,O.TreeItemCollapsibleState.Collapsed,"typeGroup",e.dbName,e.objType);if(e.contextValue==="column")return}}async getChildren(e){if(!await this.connectionManager.getConnectionString())return[];if(e){if(e.contextValue==="database")try{let s=`SELECT DISTINCT OBJTYPE FROM ${e.dbName}.._V_OBJECT_DATA WHERE DBNAME = '${e.dbName}' ORDER BY OBJTYPE`,n=await L(this.context,s,!0);return JSON.parse(n||"[]").map(o=>new Y(o.OBJTYPE,O.TreeItemCollapsibleState.Collapsed,"typeGroup",e.dbName,o.OBJTYPE))}catch(s){return O.window.showErrorMessage("Failed to load object types: "+s),[]}else if(e.contextValue==="typeGroup")try{let s=`SELECT OBJNAME, SCHEMA, OBJID, COALESCE(DESCRIPTION, '') AS DESCRIPTION FROM ${e.dbName}.._V_OBJECT_DATA WHERE DBNAME = '${e.dbName}' AND OBJTYPE = '${e.objType}' ORDER BY OBJNAME`,n=await L(this.context,s,!0);return JSON.parse(n||"[]").map(o=>{let r=["TABLE","VIEW","EXTERNAL TABLE","SYSTEM VIEW","SYSTEM TABLE"].includes(e.objType||"");return new Y(o.OBJNAME,r?O.TreeItemCollapsibleState.Collapsed:O.TreeItemCollapsibleState.None,`netezza:${e.objType}`,e.dbName,e.objType,o.SCHEMA,o.OBJID,o.DESCRIPTION)})}catch(s){return O.window.showErrorMessage("Failed to load objects: "+s),[]}else if(e.contextValue.startsWith("netezza:")&&e.objId)try{let s=`SELECT 
                        X.ATTNAME
                        , X.FORMAT_TYPE
                        , X.ATTNOTNULL::BOOL AS ATTNOTNULL
                        , X.COLDEFAULT
                        , COALESCE(X.DESCRIPTION, '') AS DESCRIPTION
                    FROM
                        ${e.dbName}.._V_RELATION_COLUMN X
                    WHERE
                        X.OBJID = ${e.objId}
                    ORDER BY 
                        X.ATTNUM`,n=await L(this.context,s,!0);return JSON.parse(n||"[]").map(o=>new Y(`${o.ATTNAME} (${o.FORMAT_TYPE})`,O.TreeItemCollapsibleState.None,"column",e.dbName,void 0,void 0,void 0,o.DESCRIPTION))}catch(s){return O.window.showErrorMessage("Failed to load columns: "+s),[]}}else try{let s=await L(this.context,"SELECT DATABASE FROM system.._v_database ORDER BY DATABASE",!0);return s?JSON.parse(s).map(i=>new Y(i.DATABASE,O.TreeItemCollapsibleState.Collapsed,"database",i.DATABASE)):(console.log("No results from database query"),[])}catch(s){return O.window.showErrorMessage("Failed to load databases: "+s),console.error(s),[]}return[]}},Y=class extends O.TreeItem{constructor(t,s,n,i,o,a,r,p){super(t,s);this.label=t;this.collapsibleState=s;this.contextValue=n;this.dbName=i;this.objType=o;this.schema=a;this.objId=r;this.objectDescription=p;let u=this.label;p&&p.trim()&&(u+=`

${p.trim()}`),a&&n.startsWith("netezza:")&&(u+=`

Schema: ${a}`),this.tooltip=u,this.description=a?`(${a})`:"",n==="database"?this.iconPath=new O.ThemeIcon("database"):n==="typeGroup"?this.iconPath=new O.ThemeIcon("folder"):n.startsWith("netezza:")?this.iconPath=this.getIconForType(o):n==="column"&&(this.iconPath=new O.ThemeIcon("symbol-field"))}getIconForType(t){switch(t){case"TABLE":return new O.ThemeIcon("table");case"VIEW":return new O.ThemeIcon("eye");case"PROCEDURE":return new O.ThemeIcon("gear");case"FUNCTION":return new O.ThemeIcon("symbol-function");case"AGGREGATE":return new O.ThemeIcon("symbol-operator");case"EXTERNAL TABLE":return new O.ThemeIcon("server");default:return new O.ThemeIcon("file")}}}});var ct={};Q(ct,{generateDDL:()=>zt});function P(l){return!l||/^[A-Z_][A-Z0-9_]*$/i.test(l)&&l===l.toUpperCase()?l:`"${l.replace(/"/g,'""')}"`}async function at(l,e,t,s){let n=`
        SELECT 
            X.OBJID::INT AS OBJID
            , X.ATTNAME
            , X.DESCRIPTION
            , CASE WHEN X.ATTNOTNULL THEN X.FORMAT_TYPE || ' NOT NULL'  ELSE X.FORMAT_TYPE END AS FULL_TYPE
            , X.ATTNOTNULL::BOOL AS ATTNOTNULL
            , X.COLDEFAULT
        FROM
            ${e.toUpperCase()}.._V_RELATION_COLUMN X
        INNER JOIN
            ${e.toUpperCase()}.._V_OBJECT_DATA D ON X.OBJID = D.OBJID
        WHERE
            X.TYPE IN ('TABLE','VIEW','EXTERNAL TABLE', 'SEQUENCE','SYSTEM VIEW','SYSTEM TABLE')
            AND X.OBJID NOT IN (4,5)
            AND D.SCHEMA = '${t.toUpperCase()}'
            AND D.OBJNAME = '${s.toUpperCase()}'
        ORDER BY 
            X.OBJID, X.ATTNUM
    `,i=await l.query(n),o=[];for(let a of i)o.push({name:a.ATTNAME,description:a.DESCRIPTION||null,fullTypeName:a.FULL_TYPE,notNull:!!a.ATTNOTNULL,defaultValue:a.COLDEFAULT||null});return o}async function Nt(l,e,t,s){try{let n=`
            SELECT ATTNAME
            FROM ${e.toUpperCase()}.._V_TABLE_DIST_MAP
            WHERE SCHEMA = '${t.toUpperCase()}'
                AND TABLENAME = '${s.toUpperCase()}'
            ORDER BY DISTSEQNO
        `;return(await l.query(n)).map(o=>o.ATTNAME)}catch{return[]}}async function Dt(l,e,t,s){try{let n=`
            SELECT ATTNAME
            FROM ${e.toUpperCase()}.._V_TABLE_ORGANIZE_COLUMN
            WHERE SCHEMA = '${t.toUpperCase()}'
                AND TABLENAME = '${s.toUpperCase()}'
            ORDER BY ORGSEQNO
        `;return(await l.query(n)).map(o=>o.ATTNAME)}catch{return[]}}async function Mt(l,e,t,s){let n=`
        SELECT 
            X.SCHEMA
            , X.RELATION
            , X.CONSTRAINTNAME
            , X.CONTYPE
            , X.ATTNAME
            , X.PKDATABASE
            , X.PKSCHEMA
            , X.PKRELATION
            , X.PKATTNAME
            , X.UPDT_TYPE
            , X.DEL_TYPE
        FROM 
            ${e.toUpperCase()}.._V_RELATION_KEYDATA X
        WHERE 
            X.OBJID NOT IN (4,5)
            AND X.SCHEMA = '${t.toUpperCase()}'
            AND X.RELATION = '${s.toUpperCase()}'
        ORDER BY
            X.SCHEMA, X.RELATION, X.CONSEQ
    `,i=new Map;try{let o=await l.query(n);for(let a of o){let r=a.CONSTRAINTNAME;if(!i.has(r)){let u={p:"PRIMARY KEY",f:"FOREIGN KEY",u:"UNIQUE"};i.set(r,{type:u[a.CONTYPE]||"UNKNOWN",typeChar:a.CONTYPE,columns:[],pkDatabase:a.PKDATABASE||null,pkSchema:a.PKSCHEMA||null,pkRelation:a.PKRELATION||null,pkColumns:[],updateType:a.UPDT_TYPE||"NO ACTION",deleteType:a.DEL_TYPE||"NO ACTION"})}let p=i.get(r);p.columns.push(a.ATTNAME),a.PKATTNAME&&p.pkColumns.push(a.PKATTNAME)}}catch(o){console.warn("Cannot retrieve keys info:",o)}return i}async function xt(l,e,t,s){try{let n=`
            SELECT DESCRIPTION
            FROM ${e.toUpperCase()}.._V_OBJECT_DATA
            WHERE SCHEMA = '${t.toUpperCase()}'
                AND OBJNAME = '${s.toUpperCase()}'
                AND OBJTYPE = 'TABLE'
        `,i=await l.query(n);if(i.length>0&&i[0].DESCRIPTION)return i[0].DESCRIPTION}catch{try{let n=`
                SELECT DESCRIPTION
                FROM ${e.toUpperCase()}.._V_OBJECT_DATA
                WHERE SCHEMA = '${t.toUpperCase()}'
                    AND OBJNAME = '${s.toUpperCase()}'
            `,i=await l.query(n);if(i.length>0&&i[0].DESCRIPTION)return i[0].DESCRIPTION}catch{}}return null}async function Ot(l,e,t,s){let n=await at(l,e,t,s);if(n.length===0)throw new Error(`Table ${e}.${t}.${s} not found or has no columns`);let i=await Nt(l,e,t,s),o=await Dt(l,e,t,s),a=await Mt(l,e,t,s),r=await xt(l,e,t,s),p=P(e),u=P(t),m=P(s),f=[];f.push(`CREATE TABLE ${p}.${u}.${m}`),f.push("(");let g=[];for(let y of n){let R=`    ${P(y.name)} ${y.fullTypeName}`;y.defaultValue&&(R+=` DEFAULT ${y.defaultValue}`),g.push(R)}if(f.push(g.join(`,
`)),i.length>0){let y=i.map(S=>P(S));f.push(`)
DISTRIBUTE ON (${y.join(", ")})`)}else f.push(`)
DISTRIBUTE ON RANDOM`);if(o.length>0){let y=o.map(S=>P(S));f.push(`ORGANIZE ON (${y.join(", ")})`)}f.push(";"),f.push("");for(let[y,S]of a){let R=P(y),D=S.columns.map(N=>P(N));if(S.typeChar==="f"){let N=S.pkColumns.filter(M=>M).map(M=>P(M));N.length>0&&f.push(`ALTER TABLE ${p}.${u}.${m} ADD CONSTRAINT ${R} ${S.type} (${D.join(", ")}) REFERENCES ${S.pkDatabase}.${S.pkSchema}.${S.pkRelation} (${N.join(", ")}) ON DELETE ${S.deleteType} ON UPDATE ${S.updateType};`)}else(S.typeChar==="p"||S.typeChar==="u")&&f.push(`ALTER TABLE ${p}.${u}.${m} ADD CONSTRAINT ${R} ${S.type} (${D.join(", ")});`)}if(r){let y=r.replace(/'/g,"''");f.push(""),f.push(`COMMENT ON TABLE ${p}.${u}.${m} IS '${y}';`)}for(let y of n)if(y.description){let S=P(y.name),R=y.description.replace(/'/g,"''");f.push(`COMMENT ON COLUMN ${p}.${u}.${m}.${S} IS '${R}';`)}return f.join(`
`)}async function Lt(l,e,t,s){let n=`
        SELECT 
            SCHEMA,
            VIEWNAME,
            DEFINITION,
            OBJID::INT
        FROM ${e.toUpperCase()}.._V_VIEW
        WHERE DATABASE = '${e.toUpperCase()}'
            AND SCHEMA = '${t.toUpperCase()}'
            AND VIEWNAME = '${s.toUpperCase()}'
    `,o=await l.query(n);if(o.length===0)throw new Error(`View ${e}.${t}.${s} not found`);let a=o[0],r=P(e),p=P(t),u=P(s),m=[];return m.push(`CREATE OR REPLACE VIEW ${r}.${p}.${u} AS`),m.push(a.DEFINITION||""),m.join(`
`)}async function $t(l,e,t,s){let n=`
        SELECT 
            SCHEMA,
            PROCEDURESOURCE,
            OBJID::INT,
            RETURNS,
            EXECUTEDASOWNER,
            DESCRIPTION,
            PROCEDURESIGNATURE,
            ARGUMENTS,
            NULL AS LANGUAGE
        FROM ${e.toUpperCase()}.._V_PROCEDURE
        WHERE DATABASE = '${e.toUpperCase()}'
            AND SCHEMA = '${t.toUpperCase()}'
            AND PROCEDURE = '${s.toUpperCase()}'
        ORDER BY 1, 2, 3
    `,o=await l.query(n);if(o.length===0)throw new Error(`Procedure ${e}.${t}.${s} not found`);let a=o[0],r={schema:a.SCHEMA,procedureSource:a.PROCEDURESOURCE,objId:a.OBJID,returns:a.RETURNS,executeAsOwner:!!a.EXECUTEDASOWNER,description:a.DESCRIPTION||null,procedureSignature:a.PROCEDURESIGNATURE,arguments:a.ARGUMENTS||null},p=P(e),u=P(t),m=P(s),f=[];if(f.push(`CREATE OR REPLACE PROCEDURE ${p}.${u}.${m}`),f.push(`RETURNS ${r.returns}`),r.executeAsOwner?f.push("EXECUTE AS OWNER"):f.push("EXECUTE AS CALLER"),f.push("LANGUAGE NZPLSQL AS"),f.push("BEGIN_PROC"),f.push(r.procedureSource),f.push("END_PROC;"),r.description){let g=r.description.replace(/'/g,"''");f.push(`COMMENT ON PROCEDURE ${m} IS '${g}';`)}return f.join(`
`)}async function Pt(l,e,t,s){let n=`
        SELECT 
            E1.SCHEMA,
            E1.TABLENAME,
            E2.EXTOBJNAME,
            E2.OBJID::INT,
            E1.DELIM,
            E1.ENCODING,
            E1.TIMESTYLE,
            E1.REMOTESOURCE,
            E1.SKIPROWS,
            E1.MAXERRORS,
            E1.ESCAPE,
            E1.LOGDIR,
            E1.DECIMALDELIM,
            E1.QUOTEDVALUE,
            E1.NULLVALUE,
            E1.CRINSTRING,
            E1.TRUNCSTRING,
            E1.CTRLCHARS,
            E1.IGNOREZERO,
            E1.TIMEEXTRAZEROS,
            E1.Y2BASE,
            E1.FILLRECORD,
            E1.COMPRESS,
            E1.INCLUDEHEADER,
            E1.LFINSTRING,
            E1.DATESTYLE,
            E1.DATEDELIM,
            E1.TIMEDELIM,
            E1.BOOLSTYLE,
            E1.FORMAT,
            E1.SOCKETBUFSIZE,
            E1.RECORDDELIM,
            E1.MAXROWS,
            E1.REQUIREQUOTES,
            E1.RECORDLENGTH,
            E1.DATETIMEDELIM,
            E1.REJECTFILE
        FROM 
            ${e.toUpperCase()}.._V_EXTERNAL E1
            JOIN ${e.toUpperCase()}.._V_EXTOBJECT E2 ON E1.DATABASE = E2.DATABASE
                AND E1.SCHEMA = E2.SCHEMA
                AND E1.TABLENAME = E2.TABLENAME
        WHERE 
            E1.DATABASE = '${e.toUpperCase()}'
            AND E1.SCHEMA = '${t.toUpperCase()}'
            AND E1.TABLENAME = '${s.toUpperCase()}'
    `,o=await l.query(n);if(o.length===0)throw new Error(`External table ${e}.${t}.${s} not found`);let a=o[0],r={schema:a.SCHEMA,tableName:a.TABLENAME,dataObject:a.EXTOBJNAME||null,delimiter:a.DELIM||null,encoding:a.ENCODING||null,timeStyle:a.TIMESTYLE||null,remoteSource:a.REMOTESOURCE||null,skipRows:a.SKIPROWS||null,maxErrors:a.MAXERRORS||null,escapeChar:a.ESCAPE||null,logDir:a.LOGDIR||null,decimalDelim:a.DECIMALDELIM||null,quotedValue:a.QUOTEDVALUE||null,nullValue:a.NULLVALUE||null,crInString:a.CRINSTRING??null,truncString:a.TRUNCSTRING??null,ctrlChars:a.CTRLCHARS??null,ignoreZero:a.IGNOREZERO??null,timeExtraZeros:a.TIMEEXTRAZEROS??null,y2Base:a.Y2BASE||null,fillRecord:a.FILLRECORD??null,compress:a.COMPRESS||null,includeHeader:a.INCLUDEHEADER??null,lfInString:a.LFINSTRING??null,dateStyle:a.DATESTYLE||null,dateDelim:a.DATEDELIM||null,timeDelim:a.TIMEDELIM||null,boolStyle:a.BOOLSTYLE||null,format:a.FORMAT||null,socketBufSize:a.SOCKETBUFSIZE||null,recordDelim:a.RECORDDELIM?String(a.RECORDDELIM).replace(/\r/g,"\\r").replace(/\n/g,"\\n"):null,maxRows:a.MAXROWS||null,requireQuotes:a.REQUIREQUOTES??null,recordLength:a.RECORDLENGTH||null,dateTimeDelim:a.DATETIMEDELIM||null,rejectFile:a.REJECTFILE||null},p=await at(l,e,t,s),u=P(e),m=P(t),f=P(s),g=[];g.push(`CREATE EXTERNAL TABLE ${u}.${m}.${f}`),g.push("(");let y=p.map(S=>`    ${P(S.name)} ${S.fullTypeName}`);return g.push(y.join(`,
`)),g.push(")"),g.push("USING"),g.push("("),r.dataObject!==null&&g.push(`    DATAOBJECT('${r.dataObject}')`),r.delimiter!==null&&g.push(`    DELIMITER '${r.delimiter}'`),r.encoding!==null&&g.push(`    ENCODING '${r.encoding}'`),r.timeStyle!==null&&g.push(`    TIMESTYLE '${r.timeStyle}'`),r.remoteSource!==null&&g.push(`    REMOTESOURCE '${r.remoteSource}'`),r.maxErrors!==null&&g.push(`    MAXERRORS ${r.maxErrors}`),r.escapeChar!==null&&g.push(`    ESCAPECHAR '${r.escapeChar}'`),r.decimalDelim!==null&&g.push(`    DECIMALDELIM '${r.decimalDelim}'`),r.logDir!==null&&g.push(`    LOGDIR '${r.logDir}'`),r.quotedValue!==null&&g.push(`    QUOTEDVALUE '${r.quotedValue}'`),r.nullValue!==null&&g.push(`    NULLVALUE '${r.nullValue}'`),r.crInString!==null&&g.push(`    CRINSTRING ${r.crInString}`),r.truncString!==null&&g.push(`    TRUNCSTRING ${r.truncString}`),r.ctrlChars!==null&&g.push(`    CTRLCHARS ${r.ctrlChars}`),r.ignoreZero!==null&&g.push(`    IGNOREZERO ${r.ignoreZero}`),r.timeExtraZeros!==null&&g.push(`    TIMEEXTRAZEROS ${r.timeExtraZeros}`),r.y2Base!==null&&g.push(`    Y2BASE ${r.y2Base}`),r.fillRecord!==null&&g.push(`    FILLRECORD ${r.fillRecord}`),r.compress!==null&&g.push(`    COMPRESS ${r.compress}`),r.includeHeader!==null&&g.push(`    INCLUDEHEADER ${r.includeHeader}`),r.lfInString!==null&&g.push(`    LFINSTRING ${r.lfInString}`),r.dateStyle!==null&&g.push(`    DATESTYLE '${r.dateStyle}'`),r.dateDelim!==null&&g.push(`    DATEDELIM '${r.dateDelim}'`),r.timeDelim!==null&&g.push(`    TIMEDELIM '${r.timeDelim}'`),r.boolStyle!==null&&g.push(`    BOOLSTYLE '${r.boolStyle}'`),r.format!==null&&g.push(`    FORMAT '${r.format}'`),r.socketBufSize!==null&&g.push(`    SOCKETBUFSIZE ${r.socketBufSize}`),r.recordDelim!==null&&g.push(`    RECORDDELIM '${r.recordDelim}'`),r.maxRows!==null&&g.push(`    MAXROWS ${r.maxRows}`),r.requireQuotes!==null&&g.push(`    REQUIREQUOTES ${r.requireQuotes}`),r.recordLength!==null&&g.push(`    RECORDLENGTH ${r.recordLength}`),r.dateTimeDelim!==null&&g.push(`    DATETIMEDELIM '${r.dateTimeDelim}'`),r.rejectFile!==null&&g.push(`    REJECTFILE '${r.rejectFile}'`),g.push(");"),g.join(`
`)}async function _t(l,e,t,s){let n=`
        SELECT 
            SCHEMA,
            OWNER,
            SYNONYM_NAME,
            REFOBJNAME,
            DESCRIPTION
        FROM ${e.toUpperCase()}.._V_SYNONYM
        WHERE DATABASE = '${e.toUpperCase()}'
            AND SCHEMA = '${t.toUpperCase()}'
            AND SYNONYM_NAME = '${s.toUpperCase()}'
    `,o=await l.query(n);if(o.length===0)throw new Error(`Synonym ${e}.${t}.${s} not found`);let a=o[0],r=P(e),p=P(a.OWNER||t),u=P(s),m=a.REFOBJNAME,f=[];if(f.push(`CREATE SYNONYM ${r}.${p}.${u} FOR ${m};`),a.DESCRIPTION){let g=a.DESCRIPTION.replace(/'/g,"''");f.push(`COMMENT ON SYNONYM ${u} IS '${g}';`)}return f.join(`
`)}async function zt(l,e,t,s,n){let i=null;try{i=await it.connect(l);let o=n.toUpperCase();return o==="TABLE"?{success:!0,ddlCode:await Ot(i,e,t,s),objectInfo:{database:e,schema:t,objectName:s,objectType:n}}:o==="VIEW"?{success:!0,ddlCode:await Lt(i,e,t,s),objectInfo:{database:e,schema:t,objectName:s,objectType:n}}:o==="PROCEDURE"?{success:!0,ddlCode:await $t(i,e,t,s),objectInfo:{database:e,schema:t,objectName:s,objectType:n}}:o==="EXTERNAL TABLE"?{success:!0,ddlCode:await Pt(i,e,t,s),objectInfo:{database:e,schema:t,objectName:s,objectType:n}}:o==="SYNONYM"?{success:!0,ddlCode:await _t(i,e,t,s),objectInfo:{database:e,schema:t,objectName:s,objectType:n}}:{success:!0,ddlCode:`-- DDL generation for ${n} not yet implemented
-- Object: ${e}.${t}.${s}
-- Type: ${n}
--
-- This feature can be extended to support:
-- - FUNCTION: Query _V_FUNCTION system table
-- - AGGREGATE: Query _V_AGGREGATE system table
`,objectInfo:{database:e,schema:t,objectName:s,objectType:n},note:`${n} DDL generation not yet implemented`}}catch(o){return{success:!1,error:`DDL generation error: ${o.message||o}`}}finally{if(i)try{await i.close()}catch{}}}var it,lt=J(()=>{"use strict";it=$(require("odbc"))});var we={};Q(we,{copyFileToClipboard:()=>ke,exportCsvToXlsb:()=>Ft,exportQueryToXlsb:()=>Ut,getTempFilePath:()=>kt});async function Ut(l,e,t,s=!1,n){let i=null;try{n&&n("Connecting to database..."),i=await Bt.connect(l),n&&n("Executing query...");let o=await i.query(e);if(!o||!o.columns||o.columns.length===0)return{success:!1,message:"Query did not return any results (no columns)"};let a=o.columns.length;n&&n(`Query returned ${a} columns`);let r=o.columns.map(M=>M.name),p=o.map(M=>r.map(k=>M[k])),u=p.length;n&&n(`Writing ${u.toLocaleString()} rows to XLSX: ${t}`);let m=H.utils.book_new(),f=[r,...p],g=H.utils.aoa_to_sheet(f);H.utils.book_append_sheet(m,g,"Query Results");let y=[["SQL Query:"],...e.split(`
`).map(M=>[M])],S=H.utils.aoa_to_sheet(y);H.utils.book_append_sheet(m,S,"SQL Code"),H.writeFile(m,t,{bookType:"xlsx",compression:!0});let D=Fe.statSync(t).size/(1024*1024);n&&(n("XLSX file created successfully"),n(`  - Rows: ${u.toLocaleString()}`),n(`  - Columns: ${a}`),n(`  - File size: ${D.toFixed(1)} MB`),n(`  - Location: ${t}`));let N={success:!0,message:`Successfully exported ${u} rows to ${t}`,details:{rows_exported:u,columns:a,file_size_mb:parseFloat(D.toFixed(1)),file_path:t}};if(s){n&&n("Copying to clipboard...");let M=await ke(t);N.details&&(N.details.clipboard_success=M)}return N}catch(o){return{success:!1,message:`Export error: ${o.message||o}`}}finally{if(i)try{await i.close()}catch{}}}async function Ft(l,e,t=!1,s,n){try{n&&n("Reading CSV content...");let i=H.read(l,{type:"string",raw:!0});if(!i.SheetNames||i.SheetNames.length===0)return{success:!1,message:"CSV content is empty or invalid"};let o=i.Sheets[i.SheetNames[0]],a=H.utils.sheet_to_json(o,{header:1});if(a.length===0)return{success:!1,message:"CSV file is empty or contains no headers"};let r=a.length-1,p=a[0]?a[0].length:0;n&&n(`Writing ${r.toLocaleString()} rows to XLSX: ${e}`);let u=H.utils.book_new(),m=H.utils.aoa_to_sheet(a);H.utils.book_append_sheet(u,m,"CSV Data");let f=s?.source||"Clipboard",g=[["CSV Source:"],[f]],y=H.utils.aoa_to_sheet(g);H.utils.book_append_sheet(u,y,"CSV Source"),H.writeFile(u,e,{bookType:"xlsx",compression:!0});let R=Fe.statSync(e).size/(1024*1024);n&&(n("XLSX file created successfully"),n(`  - Rows: ${r.toLocaleString()}`),n(`  - Columns: ${p}`),n(`  - File size: ${R.toFixed(1)} MB`),n(`  - Location: ${e}`));let D={success:!0,message:`Successfully exported ${r} rows from CSV to ${e}`,details:{rows_exported:r,columns:p,file_size_mb:parseFloat(R.toFixed(1)),file_path:e}};if(t){n&&n("Copying to clipboard...");let N=await ke(e);D.details&&(D.details.clipboard_success=N)}return D}catch(i){return{success:!1,message:`Export error: ${i.message||i}`}}}async function ke(l){return Oe.platform()!=="win32"?(console.error("Clipboard file copy is only supported on Windows"),!1):new Promise(e=>{try{let t=le.normalize(le.resolve(l)),s=`Set-Clipboard -Path "${t.replace(/"/g,'`"')}"`,n=(0,dt.spawn)("powershell.exe",["-NoProfile","-NonInteractive","-Command",s]),i="";n.stderr.on("data",o=>{i+=o.toString()}),n.on("close",o=>{o!==0?(console.error(`PowerShell clipboard copy failed: ${i}`),e(!1)):(console.log(`File copied to clipboard: ${t}`),e(!0))}),n.on("error",o=>{console.error(`Error spawning PowerShell: ${o.message}`),e(!1)})}catch(t){console.error(`Error copying file to clipboard: ${t.message}`),e(!1)}})}function kt(){let l=Oe.tmpdir(),t=`netezza_export_${Date.now()}.xlsx`;return le.join(l,t)}var H,Fe,le,Oe,dt,Bt,Ee=J(()=>{"use strict";H=$(require("xlsx")),Fe=$(require("fs")),le=$(require("path")),Oe=$(require("os")),dt=require("child_process"),Bt=require("odbc")});var pt={};Q(pt,{exportToCsv:()=>Ht});async function Ht(l,e,t,s,n){if(!Xe)throw new Error("odbc package not installed. Please run: npm install odbc");let i=await Xe.connect(e);try{n&&n.report({message:"Executing query..."});let o=await i.query(t,{cursor:!0,fetchSize:1e3});n&&n.report({message:"Writing to CSV..."});let a=ut.createWriteStream(s,{encoding:"utf8",highWaterMark:64*1024}),r=[];o.columns&&(r=o.columns.map(g=>g.name)),r.length>0&&a.write(r.map(He).join(",")+`
`);let p=0,u=[],m=[],f=100;do{u=await o.fetch();for(let g of u){p++;let y;if(r.length>0?y=r.map(S=>He(g[S])):y=Object.values(g).map(S=>He(S)),m.push(y.join(",")),m.length>=f){let S=a.write(m.join(`
`)+`
`);m=[],S||await new Promise(R=>a.once("drain",R))}}n&&u.length>0&&n.report({message:`Processed ${p} rows...`})}while(u.length>0&&!o.noData);m.length>0&&a.write(m.join(`
`)+`
`),await o.close(),a.end(),await new Promise((g,y)=>{a.on("finish",g),a.on("error",y)}),n&&n.report({message:`Completed: ${p} rows exported`})}finally{try{await i.close()}catch(o){console.error("Error closing connection:",o)}}}function He(l){if(l==null)return"";let e="";return typeof l=="bigint"?l>=Number.MIN_SAFE_INTEGER&&l<=Number.MAX_SAFE_INTEGER?e=Number(l).toString():e=l.toString():l instanceof Date?e=l.toISOString():l instanceof Buffer?e=l.toString("hex"):typeof l=="object"?e=JSON.stringify(l):e=String(l),e.includes('"')||e.includes(",")||e.includes(`
`)||e.includes("\r")?`"${e.replace(/"/g,'""')}"`:e}var ut,Xe,gt=J(()=>{"use strict";ut=$(require("fs"));try{Xe=require("odbc")}catch{console.error("odbc package not installed. Run: npm install odbc")}});var mt={};Q(mt,{ColumnTypeChooser:()=>de,NetezzaDataType:()=>G,NetezzaImporter:()=>Le,importDataToNetezza:()=>Xt});async function Xt(l,e,t,s,n){let i=Date.now();try{if(!l||!X.existsSync(l))return{success:!1,message:`Source file not found: ${l}`};if(!e)return{success:!1,message:"Target table name is required"};if(!t)return{success:!1,message:"Connection string is required"};let a=X.statSync(l).size,r=ee.extname(l).toLowerCase(),p=[".csv",".txt",".xlsx",".xlsb"];if(!p.includes(r))return{success:!1,message:`Unsupported file format: ${r}. Supported: ${p.join(", ")}`};if([".xlsx",".xlsb"].includes(r)&&!ye)return{success:!1,message:"XLSX module not available. Please run: npm install xlsx"};n?.("Starting import process..."),n?.(`  Source file: ${l}`),n?.(`  Target table: ${e}`),n?.(`  File size: ${a.toLocaleString()} bytes`),n?.(`  File format: ${r}`);let m=new Le(l,e,t);await m.analyzeDataTypes(n),n?.("Using file-based import...");let f=await m.createDataFile(n),g=m.generateCreateTableSql();if(n?.("Generated SQL:"),n?.(g),n?.("Connecting to Netezza..."),!We)throw new Error("ODBC module not available");let y=await We.connect(t);try{n?.("Executing CREATE TABLE with EXTERNAL data..."),await y.query(g),n?.("Import completed successfully")}finally{await y.close();try{X.existsSync(f)&&(X.unlinkSync(f),n?.("Temporary data file cleaned up"))}catch(R){n?.(`Warning: Could not clean up temp file: ${R.message}`)}}let S=(Date.now()-i)/1e3;return{success:!0,message:"Import completed successfully",details:{sourceFile:l,targetTable:e,fileSize:a,format:r,rowsProcessed:m.getRowsCount(),rowsInserted:m.getRowsCount(),processingTime:`${S.toFixed(1)}s`,columns:m.getSqlHeaders().length,detectedDelimiter:m.getCsvDelimiter()}}}catch(o){let a=(Date.now()-i)/1e3;return{success:!1,message:`Import failed: ${o.message}`,details:{processingTime:`${a.toFixed(1)}s`}}}}var X,ee,ye,We,G,de,Le,qe=J(()=>{"use strict";X=$(require("fs")),ee=$(require("path"));try{ye=require("xlsx")}catch{console.error("XLSX module not available")}try{We=require("odbc")}catch{console.error("ODBC module not available")}G=class{constructor(e,t,s,n){this.dbType=e;this.precision=t;this.scale=s;this.length=n}toString(){return["BIGINT","DATE","DATETIME"].includes(this.dbType)?this.dbType:this.dbType==="NUMERIC"?`${this.dbType}(${this.precision},${this.scale})`:this.dbType==="NVARCHAR"?`${this.dbType}(${this.length})`:`TODO !!! ${this.dbType}`}},de=class{constructor(){this.decimalDelimInCsv=".";this.firstTime=!0;this.currentType=new G("BIGINT")}getType(e){let t=this.currentType.dbType,s=e.length;if(t==="BIGINT"&&/^\d+$/.test(e)&&s<15)return this.firstTime=!1,new G("BIGINT");let n=(e.match(new RegExp(`\\${this.decimalDelimInCsv}`,"g"))||[]).length;if(["BIGINT","NUMERIC"].includes(t)&&n<=1){let o=e.replace(this.decimalDelimInCsv,"");if(/^\d+$/.test(o)&&s<15&&(!o.startsWith("0")||n>0))return this.firstTime=!1,new G("NUMERIC",16,6)}if((t==="DATE"||this.firstTime)&&(e.match(/-/g)||[]).length===2&&s>=8&&s<=10){let o=e.split("-");if(o.length===3&&o.every(a=>/^\d+$/.test(a)))try{let a=new Date(parseInt(o[0]),parseInt(o[1])-1,parseInt(o[2]));if(!isNaN(a.getTime()))return this.firstTime=!1,new G("DATE")}catch{}}if((t==="DATETIME"||this.firstTime)&&(e.match(/-/g)||[]).length===2&&s>=12&&s<=20){let o=e.match(/^(\d{4})-(\d{1,2})-(\d{1,2})[\s|T](\d{2}):(\d{2})(:?(\d{2}))?$/);if(o)try{let a=o[7]?parseInt(o[7]):0,r=new Date(parseInt(o[1]),parseInt(o[2])-1,parseInt(o[3]),parseInt(o[4]),parseInt(o[5]),a);if(!isNaN(r.getTime()))return this.firstTime=!1,new G("DATETIME")}catch{}}let i=Math.max(s+5,20);return this.currentType.length!==void 0&&i<this.currentType.length&&(i=this.currentType.length),this.firstTime=!1,new G("NVARCHAR",void 0,void 0,i)}refreshCurrentType(e){return this.currentType=this.getType(e),this.currentType}},Le=class{constructor(e,t,s,n){this.delimiter="	";this.delimiterPlain="\\t";this.recordDelim=`
`;this.recordDelimPlain="\\n";this.escapechar="\\";this.csvDelimiter=",";this.excelData=[];this.isExcelFile=!1;this.sqlHeaders=[];this.dataTypes=[];this.rowsCount=0;this.valuesToEscape=[];this.filePath=e,this.targetTable=t,this.connectionString=s,this.logDir=n||ee.join(ee.dirname(e),"netezza_logs");let i=ee.extname(e).toLowerCase();this.isExcelFile=[".xlsx",".xlsb"].includes(i);let o=Math.floor(Math.random()*1e3);this.pipeName=`\\\\.\\pipe\\NETEZZA_IMPORT_${o}`,this.valuesToEscape=[this.escapechar,this.recordDelim,"\r",this.delimiter],X.existsSync(this.logDir)||X.mkdirSync(this.logDir,{recursive:!0})}detectCsvDelimiter(){let t=X.readFileSync(this.filePath,"utf-8").split(`
`)[0]||"";t.startsWith("\uFEFF")&&(t=t.slice(1));let s=[";","	","|",","],n={};for(let o of s)n[o]=(t.match(new RegExp(o==="|"?"\\|":o,"g"))||[]).length;let i=Math.max(...Object.values(n));i>0&&(this.csvDelimiter=Object.keys(n).find(o=>n[o]===i)||",")}cleanColumnName(e){let t=String(e).trim();return t=t.replace(/[^0-9a-zA-Z]+/g,"_").toUpperCase(),(!t||/^\d/.test(t))&&(t="COL_"+t),t}parseCsvLine(e){let t=[],s="",n=!1;for(let i=0;i<e.length;i++){let o=e[i];o==='"'?n&&e[i+1]==='"'?(s+='"',i++):n=!n:o===this.csvDelimiter&&!n?(t.push(s),s=""):s+=o}return t.push(s),t}readExcelFile(e){if(!ye)throw new Error("XLSX module not available");e?.("Reading Excel file...");let t=ye.readFile(this.filePath,{type:"file"}),s=t.SheetNames[0];if(!s)throw new Error("Excel file has no sheets");e?.(`Processing sheet: ${s}`);let n=t.Sheets[s],o=ye.utils.sheet_to_json(n,{header:1,raw:!1,defval:""}).map(a=>a.map(r=>r!=null?String(r):""));return e?.(`Excel file loaded: ${o.length} rows, ${o[0]?.length||0} columns`),o}async analyzeDataTypes(e){e?.("Analyzing data types...");let t;if(this.isExcelFile)this.excelData=this.readExcelFile(e),t=this.excelData;else{this.detectCsvDelimiter();let n=X.readFileSync(this.filePath,"utf-8");n.startsWith("\uFEFF")&&(n=n.slice(1));let i=n.split(/\r?\n/);t=[];for(let o of i)o.trim()&&t.push(this.parseCsvLine(o))}if(!t||t.length===0)throw new Error("No data found in file");let s=[];this.sqlHeaders=t[0].map(n=>this.cleanColumnName(n||"COLUMN"));for(let n=0;n<t[0].length;n++)s.push(new de);for(let n=1;n<t.length;n++){let i=t[n];for(let o=0;o<i.length;o++)o<s.length&&i[o]&&i[o].trim()&&s[o].refreshCurrentType(i[o].trim());n%1e4===0&&e?.(`Analyzed ${n.toLocaleString()} rows...`)}return this.rowsCount=t.length-1,e?.(`Analysis complete: ${this.rowsCount.toLocaleString()} rows`),this.dataTypes=s,s}escapeValue(e){let t=String(e).trim();for(let s of this.valuesToEscape)t=t.split(s).join(`${this.escapechar}${s}`);return t}formatValue(e,t){let s=this.escapeValue(e);return t<this.dataTypes.length&&this.dataTypes[t].currentType.dbType==="DATETIME"&&(s=s.replace("T"," ")),s}generateCreateTableSql(){let e=[];for(let s=0;s<this.sqlHeaders.length;s++){let n=this.sqlHeaders[s],i=this.dataTypes[s];e.push(`        ${n} ${i.currentType.toString()}`)}let t=this.logDir.replace(/\\/g,"/");return`CREATE TABLE ${this.targetTable} AS 
(
    SELECT * FROM EXTERNAL '${this.pipeName}'
    (
${e.join(`,
`)}
    )
    USING
    (
        REMOTESOURCE 'odbc'
        DELIMITER '${this.delimiterPlain}'
        RecordDelim '${this.recordDelimPlain}'
        ESCAPECHAR '${this.escapechar}'
        NULLVALUE ''
        ENCODING 'utf-8'
        TIMESTYLE '24HOUR'
        SKIPROWS 0
        MAXERRORS 10
        LOGDIR '${t}'
    )
) DISTRIBUTE ON RANDOM;`}async createDataFile(e){let t=ee.join(this.logDir,`netezza_import_data_${Math.floor(Math.random()*1e3)}.txt`);e?.(`Creating temporary data file: ${t}`);try{let s;if(this.isExcelFile){if(!this.excelData||this.excelData.length===0)throw new Error("Excel data not loaded. Call analyzeDataTypes first.");s=this.excelData.slice(1)}else{let i=X.readFileSync(this.filePath,"utf-8");i.startsWith("\uFEFF")&&(i=i.slice(1));let o=i.split(/\r?\n/);s=[];let a=!0;for(let r of o)if(r.trim()){if(a){a=!1;continue}s.push(this.parseCsvLine(r))}}let n=[];for(let i=0;i<s.length;i++){let a=s[i].map((r,p)=>this.formatValue(r||"",p));n.push(a.join(this.delimiter)),(i+1)%1e4===0&&e?.(`Processed ${(i+1).toLocaleString()} rows...`)}return X.writeFileSync(t,n.join(this.recordDelim),"utf-8"),this.pipeName=t.replace(/\\/g,"/"),e?.(`Data file created: ${this.pipeName}`),t}catch(s){throw new Error(`Error creating data file: ${s.message}`)}}getRowsCount(){return this.rowsCount}getSqlHeaders(){return this.sqlHeaders}getCsvDelimiter(){return this.csvDelimiter}}});var ft={};Q(ft,{ClipboardDataProcessor:()=>$e,importClipboardDataToNetezza:()=>Vt});function Wt(l){let e=String(l).trim();return e=e.replace(/[^0-9a-zA-Z]+/g,"_").toUpperCase(),(!e||/^\d/.test(e))&&(e="COL_"+e),e}function qt(l,e,t){let s=String(l).trim();for(let n of t)s=s.split(n).join(`${e}${n}`);return s}function jt(l,e,t,s,n){let i=qt(l,s,n);return e<t.length&&t[e].currentType.dbType==="DATETIME"&&(i=i.replace("T"," ")),i}async function Vt(l,e,t,s,n){let i=Date.now(),o=null;try{if(!l)return{success:!1,message:"Target table name is required"};if(!e)return{success:!1,message:"Connection string is required"};n?.("Starting clipboard import process..."),n?.(`  Target table: ${l}`),n?.(`  Format preference: ${t||"auto-detect"}`);let a=new $e,[r,p]=await a.processClipboardData(t,n);if(!r||!r.length)return{success:!1,message:"No data found in clipboard"};if(r.length<2)return{success:!1,message:"Clipboard data must contain at least headers and one data row"};n?.(`  Detected format: ${p}`),n?.(`  Rows: ${r.length}`),n?.(`  Columns: ${r[0].length}`);let u=r[0].map(z=>Wt(z)),m=r.slice(1);n?.("Analyzing clipboard data types...");let f=u.map(()=>new de);for(let z=0;z<m.length;z++){let ne=m[z];for(let d=0;d<ne.length;d++)d<f.length&&ne[d]&&ne[d].trim()&&f[d].refreshCurrentType(ne[d].trim());(z+1)%1e3===0&&n?.(`Analyzed ${(z+1).toLocaleString()} rows...`)}n?.(`Analysis complete: ${m.length.toLocaleString()} data rows`);let g=je.join(require("os").tmpdir(),"netezza_clipboard_logs");K.existsSync(g)||K.mkdirSync(g,{recursive:!0});let y="	",S="\\t",R=`
`,D="\\n",N="\\",M=[N,R,"\r",y];o=je.join(g,`netezza_clipboard_import_${Math.floor(Math.random()*1e3)}.txt`),n?.(`Creating temporary data file: ${o}`);let k=[];for(let z=0;z<m.length;z++){let d=m[z].map((h,E)=>jt(h,E,f,N,M));k.push(d.join(y)),(z+1)%1e3===0&&n?.(`Processed ${(z+1).toLocaleString()} rows...`)}K.writeFileSync(o,k.join(R),"utf-8");let te=o.replace(/\\/g,"/");n?.(`Data file created: ${te}`);let ve=[];for(let z=0;z<u.length;z++)ve.push(`        ${u[z]} ${f[z].currentType.toString()}`);let Pe=g.replace(/\\/g,"/"),Te=`CREATE TABLE ${l} AS 
(
    SELECT * FROM EXTERNAL '${te}'
    (
${ve.join(`,
`)}
    )
    USING
    (
        REMOTESOURCE 'odbc'
        DELIMITER '${S}'
        RecordDelim '${D}'
        ESCAPECHAR '${N}'
        NULLVALUE ''
        ENCODING 'utf-8'
        TIMESTYLE '24HOUR'
        SKIPROWS 0
        MAXERRORS 10
        LOGDIR '${Pe}'
    )
) DISTRIBUTE ON RANDOM;`;if(n?.("Generated SQL:"),n?.(Te),n?.("Connecting to Netezza..."),!Ve)throw new Error("ODBC module not available");let be=await Ve.connect(e);try{n?.("Executing CREATE TABLE with EXTERNAL clipboard data..."),await be.query(Te),n?.("Clipboard import completed successfully")}finally{await be.close()}let Je=(Date.now()-i)/1e3;return{success:!0,message:"Clipboard import completed successfully",details:{targetTable:l,format:p,rowsProcessed:m.length,rowsInserted:m.length,processingTime:`${Je.toFixed(1)}s`,columns:u.length,detectedDelimiter:y}}}catch(a){let r=(Date.now()-i)/1e3;return{success:!1,message:`Clipboard import failed: ${a.message}`,details:{processingTime:`${r.toFixed(1)}s`}}}finally{if(o&&K.existsSync(o))try{K.unlinkSync(o),n?.("Temporary clipboard data file cleaned up")}catch(a){n?.(`Warning: Could not clean up temp file: ${a.message}`)}}}var K,je,ht,Ve,$e,wt=J(()=>{"use strict";K=$(require("fs")),je=$(require("path")),ht=$(require("vscode"));qe();try{Ve=require("odbc")}catch{console.error("ODBC module not available")}$e=class{constructor(){this.processedData=[]}processXmlSpreadsheet(e,t){t?.("Processing XML Spreadsheet data...");let s=[],n=0,i=[],o=0,a=e.match(/ExpandedColumnCount="(\d+)"/);a&&(n=parseInt(a[1]),t?.(`Table has ${n} columns`));let r=e.match(/ExpandedRowCount="(\d+)"/);r&&t?.(`Table has ${r[1]} rows`);let p=/<Row[^>]*>([\s\S]*?)<\/Row>/gi,u;for(;(u=p.exec(e))!==null;){let m=u[1];i=new Array(n).fill("");let f=/<Cell(?:\s+[^>]*ss:Index="(\d+)")?[^>]*>[\s\S]*?<Data[^>]*>([^<]*)<\/Data>[\s\S]*?<\/Cell>|<Cell(?:\s+[^>]*ss:Index="(\d+)")?[^>]*\/>/gi,g,y=0,S=/<Cell(?:[^>]*ss:Index="(\d+)")?[^>]*>(?:[\s\S]*?<Data[^>]*(?:\s+ss:Type="([^"]*)")?[^>]*>([^<]*)<\/Data>)?[\s\S]*?<\/Cell>/gi;for(;(g=S.exec(m))!==null;){g[1]&&(y=parseInt(g[1])-1);let R=g[2]||"",D=g[3]||"";R==="Boolean"&&(D=D==="0"?"False":"True"),y<n&&(i[y]=D),y++}i.some(R=>R.trim())&&s.push([...i]),o++,o%1e4===0&&t?.(`Analyzed ${o.toLocaleString()} rows...`)}return t?.(`XML processing complete: ${s.length} rows, ${n} columns`),this.processedData=s,s}processTextData(e,t){if(t?.("Processing text data..."),!e.trim())return[];let s=e.split(`
`);for(;s.length&&!s[s.length-1].trim();)s.pop();if(!s.length)return[];let n=["	",",",";","|"],i={};for(let p of n){let u=[];for(let m of s.slice(0,Math.min(5,s.length)))if(m.trim()){let f=m.split(p);u.push(f.length)}if(u.length){let m=u.reduce((g,y)=>g+y,0)/u.length,f=u.reduce((g,y)=>g+Math.pow(y-m,2),0)/u.length;i[p]=[m,-f]}}let o="	";Object.keys(i).length&&(o=Object.keys(i).reduce((p,u)=>{let[m,f]=i[p]||[0,0],[g,y]=i[u];return g>m||g===m&&y>f?u:p},"	")),t?.(`Auto-detected delimiter: '${o==="	"?"\\t":o}'`);let a=[],r=0;for(let p of s)if(p.trim()){let u=p.split(o).map(m=>m.trim());a.push(u),r=Math.max(r,u.length)}for(let p of a)for(;p.length<r;)p.push("");return t?.(`Text processing complete: ${a.length} rows, ${r} columns`),this.processedData=a,a}async getClipboardText(){return await ht.env.clipboard.readText()}async processClipboardData(e,t){t?.("Getting clipboard data...");let s=await this.getClipboardText();if(!s)throw new Error("No data found in clipboard");t?.(`Data size: ${s.length} characters`);let n="TEXT";e==="XML Spreadsheet"||!e&&s.includes("<Workbook")&&s.includes("<Worksheet")?n="XML Spreadsheet":e==="TEXT"&&(n="TEXT"),t?.(`Detected format: ${n}`);let i;return n==="XML Spreadsheet"?i=this.processXmlSpreadsheet(s,t):i=this.processTextData(s,t),t?.(`Processed ${i.length} rows`),i.length&&t?.(`Columns per row: ${i[0].length}`),[i,n]}}});var Yt={};Q(Yt,{activate:()=>Jt,deactivate:()=>Qt});module.exports=Rt(Yt);var c=$(require("vscode"));ae();_e();var ie=$(require("vscode")),Ie=class l{constructor(e,t,s){this.extensionUri=t;this.connectionManager=s;this._disposables=[];this._panel=e,this._panel.onDidDispose(()=>this.dispose(),null,this._disposables),this._panel.webview.html=this._getHtmlForWebview(this._panel.webview),this._panel.webview.onDidReceiveMessage(async n=>{switch(n.command){case"save":await this.connectionManager.saveConnection(n.data),ie.window.showInformationMessage("Connection saved!"),this.dispose();return}},null,this._disposables)}static createOrShow(e,t){let s=ie.window.activeTextEditor?ie.window.activeTextEditor.viewColumn:void 0;if(l.currentPanel){l.currentPanel._panel.reveal(s);return}let n=ie.window.createWebviewPanel("netezzaLogin","Connect to Netezza",s||ie.ViewColumn.One,{enableScripts:!0});l.currentPanel=new l(n,e,t)}dispose(){for(l.currentPanel=void 0,this._panel.dispose();this._disposables.length;){let e=this._disposables.pop();e&&e.dispose()}}_getHtmlForWebview(e){return`<!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Connect to Netezza</title>
            <style>
                body { font-family: var(--vscode-font-family); padding: 20px; color: var(--vscode-editor-foreground); background-color: var(--vscode-editor-background); }
                .form-group { margin-bottom: 15px; }
                label { display: block; margin-bottom: 5px; }
                input { width: 100%; padding: 8px; box-sizing: border-box; background: var(--vscode-input-background); color: var(--vscode-input-foreground); border: 1px solid var(--vscode-input-border); }
                button { background: var(--vscode-button-background); color: var(--vscode-button-foreground); padding: 10px 20px; border: none; cursor: pointer; }
                button:hover { background: var(--vscode-button-hoverBackground); }
            </style>
        </head>
        <body>
            <h2>Connect to Netezza</h2>
            <div class="form-group">
                <label for="host">Host</label>
                <input type="text" id="host" placeholder="nzhost">
            </div>
            <div class="form-group">
                <label for="port">Port</label>
                <input type="number" id="port" value="5480">
            </div>
            <div class="form-group">
                <label for="database">Database</label>
                <input type="text" id="database" placeholder="system">
            </div>
            <div class="form-group">
                <label for="user">User</label>
                <input type="text" id="user" placeholder="admin">
            </div>
            <div class="form-group">
                <label for="password">Password</label>
                <input type="password" id="password">
            </div>
            <button onclick="save()">Save Connection</button>

            <script>
                const vscode = acquireVsCodeApi();
                function save() {
                    const host = document.getElementById('host').value;
                    const port = document.getElementById('port').value;
                    const database = document.getElementById('database').value;
                    const user = document.getElementById('user').value;
                    const password = document.getElementById('password').value;
                    
                    vscode.postMessage({
                        command: 'save',
                        data: { host, port, database, user, password }
                    });
                }
            </script>
        </body>
        </html>`}};Ue();var F=$(require("vscode")),me=class{constructor(e){this._resultsMap=new Map;this._pinnedSources=new Set;this._pinnedResults=new Map;this._resultIdCounter=0;this._extensionUri=e}static{this.viewType="netezza.results"}resolveWebviewView(e,t,s){this._view=e,e.webview.options={enableScripts:!0,localResourceRoots:[F.Uri.joinPath(this._extensionUri,"media")]},e.webview.html=this._getHtmlForWebview(),e.webview.onDidReceiveMessage(n=>{switch(n.command){case"exportCsv":this.exportCsv(n.data);return;case"openInExcel":this.openInExcel(n.data);return;case"switchSource":this._activeSourceUri=n.sourceUri,this._updateWebview();return;case"togglePin":this._pinnedSources.has(n.sourceUri)?this._pinnedSources.delete(n.sourceUri):this._pinnedSources.add(n.sourceUri),this._updateWebview();return;case"toggleResultPin":this._toggleResultPin(n.sourceUri,n.resultSetIndex);return;case"switchToPinnedResult":this._switchToPinnedResult(n.resultId);return;case"unpinResult":this._pinnedResults.delete(n.resultId),this._updateWebview();return;case"closeSource":this.closeSource(n.sourceUri);return;case"copyToClipboard":F.env.clipboard.writeText(n.text),F.window.showInformationMessage("Copied to clipboard");return;case"info":F.window.showInformationMessage(n.text);return;case"error":F.window.showErrorMessage(n.text);return}})}setActiveSource(e){this._resultsMap.has(e)&&this._activeSourceUri!==e&&(this._activeSourceUri=e,this._updateWebview())}updateResults(e,t,s=!1){this._resultsMap.has(t)||this._pinnedSources.add(t);let n=[];Array.isArray(e)?n=e:n=[e];let i=this._resultsMap.get(t)||[],o=Array.from(this._pinnedResults.entries()).filter(([p,u])=>u.sourceUri===t).sort((p,u)=>p[1].resultSetIndex-u[1].resultSetIndex),a=[],r=[];o.forEach(([p,u])=>{u.resultSetIndex<i.length&&(a.push(i[u.resultSetIndex]),r.push([p,u]))}),a.push(...n),r.forEach(([p,u],m)=>{let f=this._pinnedResults.get(p);f&&(f.resultSetIndex=m)}),s||Array.from(this._resultsMap.keys()).filter(u=>u!==t&&!this._pinnedSources.has(u)).forEach(u=>{this._resultsMap.delete(u),Array.from(this._pinnedResults.entries()).filter(([f,g])=>g.sourceUri===u).map(([f,g])=>f).forEach(f=>this._pinnedResults.delete(f))}),this._resultsMap.set(t,a),this._activeSourceUri=t,this._view?(this._updateWebview(),this._view.show?.(!0)):F.window.showInformationMessage('Query completed. Please open "Query Results" panel to view data.')}_updateWebview(){this._view&&(this._view.webview.html=this._getHtmlForWebview())}_toggleResultPin(e,t){let s=Array.from(this._pinnedResults.entries()).find(([n,i])=>i.sourceUri===e&&i.resultSetIndex===t);if(s)this._pinnedResults.delete(s[0]);else{let n=`result_${++this._resultIdCounter}`,i=Date.now(),a=`${e.split(/[\\/]/).pop()||e} - Result ${t+1}`;this._pinnedResults.set(n,{sourceUri:e,resultSetIndex:t,timestamp:i,label:a})}this._updateWebview()}_switchToPinnedResult(e){let t=this._pinnedResults.get(e);t&&(this._activeSourceUri=t.sourceUri,this._updateWebview(),this._view&&this._view.webview.postMessage({command:"switchToResultSet",resultSetIndex:t.resultSetIndex}))}async exportCsv(e){let t=await F.window.showSaveDialog({filters:{"CSV Files":["csv"]},saveLabel:"Export"});t&&(await F.workspace.fs.writeFile(t,Buffer.from(e)),F.window.showInformationMessage(`Results exported to ${t.fsPath}`))}async openInExcel(e){F.commands.executeCommand("netezza.exportCurrentResultToXlsbAndOpen",e)}closeSource(e){if(this._resultsMap.has(e)){if(this._resultsMap.delete(e),this._pinnedSources.delete(e),Array.from(this._pinnedResults.entries()).filter(([s,n])=>n.sourceUri===e).map(([s,n])=>s).forEach(s=>this._pinnedResults.delete(s)),this._activeSourceUri===e){let s=Array.from(this._resultsMap.keys());this._activeSourceUri=s.length>0?s[0]:void 0}this._updateWebview()}}_getHtmlForWebview(){if(!this._view)return"";let{scriptUri:e,virtualUri:t,mainScriptUri:s,styleUri:n}=this._getScriptUris(),i=this._prepareViewData();return this._buildHtmlDocument(e,t,s,n,i)}_getScriptUris(){return{scriptUri:this._view.webview.asWebviewUri(F.Uri.joinPath(this._extensionUri,"media","tanstack-table-core.js")),virtualUri:this._view.webview.asWebviewUri(F.Uri.joinPath(this._extensionUri,"media","tanstack-virtual-core.js")),mainScriptUri:this._view.webview.asWebviewUri(F.Uri.joinPath(this._extensionUri,"media","resultPanel.js")),styleUri:this._view.webview.asWebviewUri(F.Uri.joinPath(this._extensionUri,"media","resultPanel.css"))}}_prepareViewData(){let e=Array.from(this._resultsMap.keys()),t=Array.from(this._pinnedSources),s=Array.from(this._pinnedResults.entries()).map(([a,r])=>({id:a,...r})),n=this._activeSourceUri&&this._resultsMap.has(this._activeSourceUri)?this._activeSourceUri:e.length>0?e[0]:null,i=n?this._resultsMap.get(n):[],o=(a,r)=>typeof r=="bigint"?r>=Number.MIN_SAFE_INTEGER&&r<=Number.MAX_SAFE_INTEGER?Number(r):r.toString():r;return{sourcesJson:JSON.stringify(e),pinnedSourcesJson:JSON.stringify(t),pinnedResultsJson:JSON.stringify(s),activeSourceJson:JSON.stringify(n),resultSetsJson:JSON.stringify(i,o)}}_buildHtmlDocument(e,t,s,n,i){let o=this._view.webview.cspSource;return`<!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src ${o} 'unsafe-inline'; style-src ${o} 'unsafe-inline';">
            <title>Query Results</title>
            <script src="${e}"></script>
            <script src="${t}"></script>
            <link rel="stylesheet" href="${n}">
        </head>
        <body>
            <div id="sourceTabs" class="source-tabs"></div>
            <div id="resultSetTabs" class="result-set-tabs" style="display: none;"></div>
            
            <div class="controls">
                <button onclick="toggleRowView()" title="Toggle Row View">\u{1F441}\uFE0F Row View</button>
                <button onclick="openInExcel()" title="Open results in Excel">\u{1F4CA} Excel</button>
                <button onclick="exportToCsv()" title="Export results to CSV">\u{1F4C4} CSV</button>
                <div style="width: 1px; height: 16px; background: var(--vscode-panel-border); margin: 0 4px;"></div>
                <button onclick="selectAll()" title="Select all rows">\u2611\uFE0F Select All</button>
                <button onclick="copySelection(false)" title="Copy selected cells to clipboard">\u{1F4CB} Copy</button>
                <button onclick="copySelection(true)" title="Copy selected cells with headers">\u{1F4CB} Copy w/ Headers</button>
                <input type="text" id="globalFilter" placeholder="Filter..." onkeyup="onFilterChanged()" style="background: var(--vscode-input-background); color: var(--vscode-input-foreground); border: 1px solid var(--vscode-input-border); padding: 4px;">
                <span id="rowCountInfo" style="margin-left: auto; font-size: 12px; opacity: 0.8;"></span>
            </div>

            <div id="groupingPanel" class="grouping-panel" ondragover="onDragOverGroup(event)" ondragleave="onDragLeaveGroup(event)" ondrop="onDropGroup(event)">
                <span style="opacity: 0.5;">Drag headers here to group</span>
            </div>

            <div id="mainSplitView" class="main-split-view">
                <div id="gridContainer"></div>
                <div id="rowViewPanel" class="row-view-panel">
                    <div class="row-view-header">
                        <span>Row Details & Comparison</span>
                        <span class="row-view-close" onclick="toggleRowView()">\xD7</span>
                    </div>
                    <div id="rowViewContent" class="row-view-content">
                        <div class="row-view-placeholder">Select 1 or 2 rows to view details or compare</div>
                    </div>
                </div>
            </div>
            
            <script>
                const vscode = acquireVsCodeApi();
                window.sources = ${i.sourcesJson};
                window.pinnedSources = new Set(${i.pinnedSourcesJson});
                window.pinnedResults = ${i.pinnedResultsJson};
                window.activeSource = ${i.activeSourceJson};
                window.resultSets = ${i.resultSetsJson};
                
                let grids = [];
                let activeGridIndex = 0;
            </script>
            <script src="${s}"></script>
            <script>
                // Initialize on load
                init();
            </script>
        </body>
        </html>`}};var U=$(require("vscode"));ae();var Ne=class{constructor(e){this.context=e;this.schemaCache=new Map;this.tableCache=new Map;this.columnCache=new Map;this.tableIdMap=new Map}async provideCompletionItems(e,t,s,n){let i=e.getText(),o=this.stripComments(i),a=this.parseLocalDefinitions(o),r=e.lineAt(t).text.substr(0,t.character),p=r.toUpperCase();if(/(FROM|JOIN)\s+$/.test(p)){let g=await this.getDatabases();return[...a.map(S=>{let R=new U.CompletionItem(S.name,U.CompletionItemKind.Class);return R.detail=S.type,R}),...g]}let u=r.match(/(?:FROM|JOIN)\s+([a-zA-Z0-9_]+)\.\s*$/i);if(u){let g=u[1],y=await this.getSchemas(g);return new U.CompletionList(y,!1)}let m=r.match(/(?:FROM|JOIN)\s+([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\.$/i);if(m){let g=m[1],y=m[2];return this.getTables(g,y)}let f=r.match(/(?:FROM|JOIN)\s+([a-zA-Z0-9_]+)\.\.$/i);if(f){let g=f[1];return this.getTables(g,void 0)}if(r.trim().endsWith(".")){let g=r.trim().split(/[\s.]+/),y=r.match(/([a-zA-Z0-9_]+)\.$/);if(y){let S=y[1],R=this.findAlias(o,S);if(R){let N=a.find(M=>M.name.toUpperCase()===R.table.toUpperCase());return N?N.columns.map(M=>{let k=new U.CompletionItem(M,U.CompletionItemKind.Field);return k.detail="Local Column",k}):this.getColumns(R.db,R.schema,R.table)}let D=a.find(N=>N.name.toUpperCase()===S.toUpperCase());if(D)return D.columns.map(N=>{let M=new U.CompletionItem(N,U.CompletionItemKind.Field);return M.detail="Local Column",M})}}return this.getKeywords()}stripComments(e){let t=e.replace(/--.*$/gm,"");return t=t.replace(/\/\*[\s\S]*?\*\//g,""),t}parseLocalDefinitions(e){let t=[],s=/CREATE\s+TABLE\s+([a-zA-Z0-9_]+)\s+AS\s*\(/gi,n;for(;(n=s.exec(e))!==null;){let a=n[1],r=n.index+n[0].length,p=this.extractBalancedParenthesisContent(e,r);if(p){let u=this.extractColumnsFromQuery(p);t.push({name:a,type:"Temp Table",columns:u})}}let i=/\bWITH\s+/gi;for(;(n=i.exec(e))!==null;){let a=n.index+n[0].length;for(;;){let r=/^\s*([a-zA-Z0-9_]+)\s+AS\s*\(/i,p=e.substring(a),u=p.match(r);if(!u)break;let m=u[1],f=a+u[0].length,g=this.extractBalancedParenthesisContent(e,a+u[0].length-1),y=p.indexOf("(",u.index+u[1].length),S=a+y,R=this.extractBalancedParenthesisContent(e,S+1);if(R){let D=this.extractColumnsFromQuery(R);t.push({name:m,type:"CTE",columns:D}),a=S+1+R.length+1;let N=/^\s*,/,M=e.substring(a);if(N.test(M)){let k=M.match(N);a+=k[0].length}else break}else break}}let o=/\bJOIN\s+\(/gi;for(;(n=o.exec(e))!==null;){let a=n.index+n[0].length,r=this.extractBalancedParenthesisContent(e,a);if(r&&/^\s*SELECT\b/i.test(r)){let p=a+r.length+1,m=e.substring(p).match(/^\s+(?:AS\s+)?([a-zA-Z0-9_]+)/i);if(m){let f=m[1],g=this.extractColumnsFromQuery(r);t.push({name:f,type:"Subquery",columns:g})}}}return t}extractBalancedParenthesisContent(e,t){let s=1,n=t;for(;n<e.length;n++)if(e[n]==="("?s++:e[n]===")"&&s--,s===0)return e.substring(t,n);return null}extractColumnsFromQuery(e){let t=e.match(/^\s*SELECT\s+/i);if(!t)return[];let s="",n=0,i=-1,o=t[0].length;for(let p=o;p<e.length;p++)if(e[p]==="("?n++:e[p]===")"&&n--,n===0&&e.substr(p).match(/^\s+FROM\b/i)){i=p;break}i!==-1?s=e.substring(o,i):s=e.substring(o);let a=[],r="";n=0;for(let p=0;p<s.length;p++){let u=s[p];u==="("?n++:u===")"&&n--,u===","&&n===0?(a.push(r.trim()),r=""):r+=u}return r.trim()&&a.push(r.trim()),a.map(p=>{let u=p.match(/\s+AS\s+([a-zA-Z0-9_]+)$/i);if(u)return u[1];let m=p.match(/\s+([a-zA-Z0-9_]+)$/i);if(m)return m[1];let f=p.split(".");return f[f.length-1]})}getKeywords(){return["SELECT","FROM","WHERE","GROUP BY","ORDER BY","LIMIT","INSERT","INTO","VALUES","UPDATE","SET","DELETE","CREATE","DROP","TABLE","VIEW","DATABASE","JOIN","INNER","LEFT","RIGHT","OUTER","ON","AND","OR","NOT","NULL","IS","IN","BETWEEN","LIKE","AS","DISTINCT","CASE","WHEN","THEN","ELSE","END","WITH","UNION","ALL"].map(t=>{let s=new U.CompletionItem(t,U.CompletionItemKind.Keyword);return s.detail="SQL Keyword",s})}async getDatabases(){if(this.dbCache)return this.dbCache;try{let t=await L(this.context,"SELECT DATABASE FROM system.._v_database ORDER BY DATABASE",!0);if(!t)return[];let s=JSON.parse(t);return this.dbCache=s.map(n=>{let i=new U.CompletionItem(n.DATABASE,U.CompletionItemKind.Module);return i.detail="Database",i}),this.dbCache}catch(e){return console.error(e),[]}}async getSchemas(e){if(this.schemaCache.has(e))return this.schemaCache.get(e);try{let t=`SELECT DISTINCT SCHEMA FROM ${e}.._V_OBJECT_DATA WHERE SCHEMA IS NOT NULL ORDER BY SCHEMA LIMIT 500`,s=await L(this.context,t,!0);if(!s)return[];let i=JSON.parse(s).filter(o=>o.SCHEMA!=null&&o.SCHEMA!=="").map(o=>{let a=o.SCHEMA,r=new U.CompletionItem(a,U.CompletionItemKind.Folder);return r.detail=`Schema in ${e}`,r.insertText=a,r.sortText=a,r.filterText=a,r});return this.schemaCache.set(e,i),i}catch(t){return console.error("[SqlCompletion] Error in getSchemas:",t),[]}}async getTables(e,t){let s=t?`${e}.${t}`:`${e}..`;if(this.tableCache.has(s))return this.tableCache.get(s);try{let n="";t?n=`SELECT OBJNAME, OBJID FROM ${e}.._V_OBJECT_DATA WHERE SCHEMA='${t}' AND OBJTYPE='TABLE' ORDER BY OBJNAME LIMIT 1000`:n=`SELECT OBJNAME, OBJID, SCHEMA FROM ${e}.._V_OBJECT_DATA WHERE OBJTYPE='TABLE' ORDER BY OBJNAME LIMIT 1000`;let i=await L(this.context,n,!0);if(!i)return[];let a=JSON.parse(i).map(r=>{let p=new U.CompletionItem(r.OBJNAME,U.CompletionItemKind.Class);p.detail=t?"Table":`Table (${r.SCHEMA})`;let u=t?`${e}.${t}.${r.OBJNAME}`:`${e}..${r.OBJNAME}`;return!t&&r.SCHEMA&&this.tableIdMap.set(`${e}.${r.SCHEMA}.${r.OBJNAME}`,r.OBJID),this.tableIdMap.set(u,r.OBJID),p});return this.tableCache.set(s,a),a}catch(n){return console.error(n),[]}}async getColumns(e,t,s){let n,i=e?`${e}..`:"";t&&e?n=this.tableIdMap.get(`${e}.${t}.${s}`):e&&(n=this.tableIdMap.get(`${e}..${s}`));let o=`${e||"CURRENT"}.${t||""}.${s}`;if(this.columnCache.has(o))return this.columnCache.get(o);try{let a="";if(n)a=`SELECT ATTNAME, FORMAT_TYPE FROM ${i}_V_RELATION_COLUMN WHERE OBJID = ${n} ORDER BY ATTNUM`;else{let m=t?`AND SCHEMA='${t}'`:"";a=`
                    SELECT C.ATTNAME, C.FORMAT_TYPE 
                    FROM ${i}_V_RELATION_COLUMN C
                    JOIN ${i}_V_OBJECT_DATA O ON C.OBJID = O.OBJID
                    WHERE O.OBJNAME = '${s}' ${m}
                    ORDER BY C.ATTNUM
                `}let r=await L(this.context,a,!0);if(!r)return[];let u=JSON.parse(r).map(m=>{let f=new U.CompletionItem(m.ATTNAME,U.CompletionItemKind.Field);return f.detail=m.FORMAT_TYPE,f});return this.columnCache.set(o,u),u}catch(a){return console.error(a),[]}}findAlias(e,t){let s=new RegExp(`([a-zA-Z0-9_]+)\\.([a-zA-Z0-9_]+)\\.([a-zA-Z0-9_]+)\\s+(?:AS\\s+)?${t}\\b`,"i"),n=e.match(s);if(n)return{db:n[1],schema:n[2],table:n[3]};let i=new RegExp(`([a-zA-Z0-9_]+)\\.\\.([a-zA-Z0-9_]+)\\s+(?:AS\\s+)?${t}\\b`,"i"),o=e.match(i);if(o)return{db:o[1],schema:void 0,table:o[2]};let a=new RegExp(`(?:FROM|JOIN|,)\\s+([a-zA-Z0-9_]+)\\s+(?:AS\\s+)?${t}\\b`,"i"),r=e.match(a);if(r)return{db:void 0,schema:void 0,table:r[1]}}};var rt=$(require("vscode"));ae();var he=class{constructor(e,t){this._extensionUri=e;this.context=t}static{this.viewType="netezza.search"}resolveWebviewView(e,t,s){this._view=e,e.webview.options={enableScripts:!0,localResourceRoots:[this._extensionUri]},e.webview.html=this._getHtmlForWebview(e.webview),e.webview.onDidReceiveMessage(async n=>{switch(n.type){case"search":await this.search(n.value);break;case"navigate":rt.commands.executeCommand("netezza.revealInSchema",n);break}})}async search(e){if(!e||e.length<2)return;let s=`%${e.replace(/'/g,"''")}%`,n=`
            SELECT OBJNAME AS NAME, SCHEMA, OBJTYPE AS TYPE, '' AS PARENT, 
                   COALESCE(DESCRIPTION, '') AS DESCRIPTION, 'NAME' AS MATCH_TYPE
            FROM _V_OBJECT_DATA 
            WHERE OBJNAME LIKE '${s}'
            UNION ALL
            SELECT C.ATTNAME AS NAME, O.SCHEMA, 'COLUMN' AS TYPE, O.OBJNAME AS PARENT,
                   COALESCE(C.DESCRIPTION, '') AS DESCRIPTION, 'NAME' AS MATCH_TYPE
            FROM _V_RELATION_COLUMN C
            JOIN _V_OBJECT_DATA O ON C.OBJID = O.OBJID
            WHERE C.ATTNAME LIKE '${s}'
            UNION ALL
            SELECT V.VIEWNAME AS NAME, V.SCHEMA, 'VIEW' AS TYPE, '' AS PARENT,
                   'Found in view definition' AS DESCRIPTION, 'DEFINITION' AS MATCH_TYPE
            FROM _V_VIEW V
            WHERE V.DEFINITION LIKE '${s}'
            UNION ALL
            SELECT P.PROCEDURE AS NAME, P.SCHEMA, 'PROCEDURE' AS TYPE, '' AS PARENT,
                   'Found in procedure source' AS DESCRIPTION, 'SOURCE' AS MATCH_TYPE
            FROM _V_PROCEDURE P
            WHERE P.PROCEDURESOURCE LIKE '${s}'
            ORDER BY TYPE, NAME
            LIMIT 100
        `;try{let i=await L(this.context,n,!0);this._view&&this._view.webview.postMessage({type:"results",data:i?JSON.parse(i):[]})}catch(i){this._view&&this._view.webview.postMessage({type:"error",message:i.message})}}_getHtmlForWebview(e){return`<!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Schema Search</title>
            <style>
                body { font-family: var(--vscode-font-family); padding: 10px; color: var(--vscode-foreground); }
                .search-box { display: flex; gap: 5px; margin-bottom: 10px; }
                input { flex-grow: 1; padding: 5px; background: var(--vscode-input-background); color: var(--vscode-input-foreground); border: 1px solid var(--vscode-input-border); }
                button { 
                    display: inline-flex; 
                    align-items: center; 
                    justify-content: center; 
                    gap: 6px; 
                    background-color: var(--vscode-button-secondaryBackground); 
                    color: var(--vscode-button-secondaryForeground); 
                    border: 1px solid var(--vscode-contrastBorder, transparent); 
                    padding: 4px 10px; 
                    cursor: pointer; 
                    border-radius: 2px; 
                    font-family: var(--vscode-font-family); 
                    font-size: 12px; 
                    line-height: 18px; 
                }
                button:hover { background-color: var(--vscode-button-secondaryHoverBackground); }
                button.primary { background-color: var(--vscode-button-background); color: var(--vscode-button-foreground); }
                button.primary:hover { background-color: var(--vscode-button-hoverBackground); }
                .results { list-style: none; padding: 0; }
                .result-item { padding: 5px; border-bottom: 1px solid var(--vscode-panel-border); display: flex; flex-direction: column; cursor: pointer; position: relative; }
                .result-item:hover { background: var(--vscode-list-hoverBackground); }
                .item-header { display: flex; justify-content: space-between; font-weight: bold; }
                .item-details { font-size: 0.9em; opacity: 0.8; display: flex; gap: 10px; }
                .type-badge { font-size: 0.8em; background: var(--vscode-badge-background); color: var(--vscode-badge-foreground); padding: 2px 5px; border-radius: 3px; }
                .tooltip { position: absolute; background: var(--vscode-editorHoverWidget-background); color: var(--vscode-editorHoverWidget-foreground); border: 1px solid var(--vscode-editorHoverWidget-border); padding: 8px; border-radius: 4px; font-size: 0.9em; max-width: 300px; word-wrap: break-word; z-index: 1000; opacity: 0; visibility: hidden; transition: opacity 0.2s, visibility 0.2s; pointer-events: none; }
                .result-item:hover .tooltip { opacity: 1; visibility: visible; }
                .tooltip.top { bottom: 100%; left: 0; margin-bottom: 5px; }
                .tooltip.bottom { top: 100%; left: 0; margin-top: 5px; }
            </style>
        </head>
        <body>
            <div class="search-box">
                <input type="text" id="searchInput" placeholder="Search tables, columns, view definitions, procedure source..." />
                <button id="searchBtn" class="primary">Search</button>
            </div>
            <div id="status"></div>
            <ul class="results" id="resultsList"></ul>

            <script>
                const vscode = acquireVsCodeApi();
                const searchInput = document.getElementById('searchInput');
                const searchBtn = document.getElementById('searchBtn');
                const resultsList = document.getElementById('resultsList');
                const status = document.getElementById('status');

                searchBtn.addEventListener('click', () => {
                    const term = searchInput.value;
                    if (term) {
                        status.textContent = 'Searching...';
                        resultsList.innerHTML = '';
                        vscode.postMessage({ type: 'search', value: term });
                    }
                });

                searchInput.addEventListener('keypress', (e) => {
                    if (e.key === 'Enter') {
                        searchBtn.click();
                    }
                });

                window.addEventListener('message', event => {
                    const message = event.data;
                    switch (message.type) {
                        case 'results':
                            status.textContent = '';
                            renderResults(message.data);
                            break;
                        case 'error':
                            status.textContent = 'Error: ' + message.message;
                            break;
                    }
                });

                function renderResults(data) {
                    if (!data || data.length === 0) {
                        status.textContent = 'No results found.';
                        return;
                    }
                    
                    data.forEach(item => {
                        const li = document.createElement('li');
                        li.className = 'result-item';
                        
                        const parentInfo = item.PARENT ? \`Parent: \${item.PARENT}\` : '';
                        const schemaInfo = item.SCHEMA ? \`Schema: \${item.SCHEMA}\` : '';
                        const description = item.DESCRIPTION && item.DESCRIPTION.trim() ? item.DESCRIPTION : '';
                        
                        // Add match type indicator
                        const matchTypeInfo = item.MATCH_TYPE === 'DEFINITION' ? 'Match in view definition' :
                                            item.MATCH_TYPE === 'SOURCE' ? 'Match in procedure source' :
                                            item.MATCH_TYPE === 'NAME' ? 'Match in name' : '';
                        
                        li.innerHTML = \`
                            <div class="item-header">
                                <span>\${item.NAME}</span>
                                <span class="type-badge">\${item.TYPE}</span>
                            </div>
                            <div class="item-details">
                                <span>\${schemaInfo}</span>
                                <span>\${parentInfo}</span>
                                \${matchTypeInfo ? \`<span style="font-style: italic; color: var(--vscode-descriptionForeground);">\${matchTypeInfo}</span>\` : ''}
                            </div>
                            \${description ? \`<div class="tooltip bottom">\${description}</div>\` : ''}
                        \`;
                        
                        // Add double-click handler to navigate to schema tree
                        li.addEventListener('dblclick', () => {
                            vscode.postMessage({ 
                                type: 'navigate', 
                                name: item.NAME,
                                schema: item.SCHEMA,
                                objType: item.TYPE,
                                parent: item.PARENT
                            });
                        });
                        
                        resultsList.appendChild(li);
                    });
                }
            </script>
        </body>
        </html>`}};var Z=class{static splitStatements(e){let t=[],s="",n=!1,i=!1,o=!1,a=!1,r=0;for(;r<e.length;){let p=e[r],u=r+1<e.length?e[r+1]:"";if(o)p===`
`&&(o=!1);else if(a){if(p==="*"&&u==="/"){a=!1,s+=p+u,r++,r++;continue}}else if(n)p==="'"&&e[r-1]!=="\\"&&(n=!1);else if(i)p==='"'&&e[r-1]!=="\\"&&(i=!1);else if(p==="-"&&u==="-")o=!0;else if(p==="/"&&u==="*")a=!0;else if(p==="'")n=!0;else if(p==='"')i=!0;else if(p===";"){s.trim()&&t.push(s.trim()),s="",r++;continue}s+=p,r++}return s.trim()&&t.push(s.trim()),t}static getStatementAtPosition(e,t){let s=0,n=e.length,i=!1,o=!1,a=!1,r=!1,p=-1;for(let m=0;m<t;m++){let f=e[m],g=m+1<e.length?e[m+1]:"";a?f===`
`&&(a=!1):r?f==="*"&&g==="/"&&(r=!1,m++):i?f==="'"&&e[m-1]!=="\\"&&(i=!1):o?f==='"'&&e[m-1]!=="\\"&&(o=!1):f==="-"&&g==="-"?a=!0:f==="/"&&g==="*"?r=!0:f==="'"?i=!0:f==='"'?o=!0:f===";"&&(p=m)}s=p+1,i=!1,o=!1,a=!1,r=!1;for(let m=s;m<e.length;m++){let f=e[m],g=m+1<e.length?e[m+1]:"";if(a)f===`
`&&(a=!1);else if(r)f==="*"&&g==="/"&&(r=!1,m++);else if(i)f==="'"&&e[m-1]!=="\\"&&(i=!1);else if(o)f==='"'&&e[m-1]!=="\\"&&(o=!1);else if(f==="-"&&g==="-")a=!0;else if(f==="/"&&g==="*")r=!0;else if(f==="'")i=!0;else if(f==='"')o=!0;else if(f===";"){n=m;break}}let u=e.substring(s,n).trim();return u?{sql:u,start:s,end:n}:null}static getObjectAtPosition(e,t){let s=p=>/[a-zA-Z0-9_."]/i.test(p),n=t;for(;n>0&&s(e[n-1]);)n--;let i=t;for(;i<e.length&&s(e[i]);)i++;let o=e.substring(n,i);if(!o)return null;let a=p=>p?p.replace(/"/g,""):void 0;if(o.includes("..")){let p=o.split("..");if(p.length===2)return{database:a(p[0]),name:a(p[1])}}let r=o.split(".");return r.length===1?{name:a(r[0])}:r.length===2?{schema:a(r[0]),name:a(r[1])}:r.length===3?{database:a(r[0]),schema:a(r[1]),name:a(r[2])}:null}};var ce=$(require("vscode"));var De=class{provideDocumentLinks(e,t){let s=[],n=e.getText(),i=/[a-zA-Z0-9_"]+(\.[a-zA-Z0-9_"]*)+/g,o;for(;(o=i.exec(n))!==null;){let a=e.positionAt(o.index),r=e.positionAt(o.index+o[0].length),p=new ce.Range(a,r),u=Z.getObjectAtPosition(n,o.index+Math.floor(o[0].length/2));if(u){if(o[0].split(".").length===2&&!u.database&&this.isLikelyAliasReference(n,o.index))continue;let g={name:u.name,schema:u.schema,database:u.database},y=ce.Uri.parse(`command:netezza.revealInSchema?${encodeURIComponent(JSON.stringify(g))}`),S=new ce.DocumentLink(p,y);S.tooltip=`Reveal ${u.name} in Schema`,s.push(S)}}return s}isLikelyAliasReference(e,t){let n=e.substring(Math.max(0,t-200),t).replace(/--[^\n]*/g,"").replace(/\/\*[\s\S]*?\*\//g,"").toUpperCase();return/(?:FROM|JOIN)\s+[a-zA-Z0-9_"]*$/i.test(n)?!1:!!n.match(/\b(SELECT|WHERE|ON|HAVING|ORDER\s+BY|GROUP\s+BY|AND|OR|SET|VALUES)\b(?!.*\b(?:FROM|JOIN)\b)/)}};var xe=$(require("vscode")),Me=class{provideFoldingRanges(e,t,s){let n=[],i=[],o=/^\s*--\s*REGION\b/i,a=/^\s*--\s*ENDREGION\b/i;for(let r=0;r<e.lineCount;r++){let u=e.lineAt(r).text;if(o.test(u))i.push(r);else if(a.test(u)&&i.length>0){let m=i.pop();n.push(new xe.FoldingRange(m,r,xe.FoldingRangeKind.Region))}}return n}};var j=$(require("vscode"));Ae();var fe=class{constructor(e,t){this._extensionUri=e;this._context=t}static{this.viewType="netezza.queryHistory"}resolveWebviewView(e,t,s){this._view=e,e.webview.options={enableScripts:!0,localResourceRoots:[this._extensionUri]},e.webview.html=this._getHtmlForWebview(e.webview),this.sendHistoryToWebview(),e.webview.onDidReceiveMessage(async n=>{switch(n.type){case"refresh":this.refresh();break;case"clearAll":await this.clearAllHistory();break;case"deleteEntry":await this.deleteEntry(n.id,n.query);break;case"copyQuery":await j.env.clipboard.writeText(n.query),j.window.showInformationMessage("Query copied to clipboard");break;case"executeQuery":await this.executeQuery(n.query);break;case"getHistory":await this.sendHistoryToWebview();break;case"toggleFavorite":await this.toggleFavorite(n.id);break;case"updateEntry":await this.updateEntry(n.id,n.tags,n.description);break;case"requestEdit":await this.requestEdit(n.id);break;case"requestTagFilter":await this.requestTagFilter(n.tags);break;case"showFavoritesOnly":await this.sendFavoritesToWebview();break;case"filterByTag":await this.sendFilteredByTagToWebview(n.tag);break}})}refresh(){this._view&&this.sendHistoryToWebview()}async sendHistoryToWebview(){if(!this._view)return;let e=new q(this._context),t=await e.getHistory(),s=await e.getStats();console.log("QueryHistoryView: sending history to webview, entries=",t.length),this._view.webview.postMessage({type:"historyData",history:t,stats:s})}async clearAllHistory(){await j.window.showWarningMessage("Are you sure you want to clear all query history?",{modal:!0},"Clear All")==="Clear All"&&(await new q(this._context).clearHistory(),this.refresh(),j.window.showInformationMessage("Query history cleared"))}async deleteEntry(e,t){let s=t?`: ${t.substring(0,50)}${t.length>50?"...":""}`:"";await j.window.showWarningMessage(`Are you sure you want to delete this query${s}?`,{modal:!0},"Delete")==="Delete"&&(await new q(this._context).deleteEntry(e),this.refresh())}async executeQuery(e){let t=await j.workspace.openTextDocument({content:e,language:"sql"});await j.window.showTextDocument(t)}async toggleFavorite(e){await new q(this._context).toggleFavorite(e),this.refresh()}async updateEntry(e,t,s){await new q(this._context).updateEntry(e,t,s),this.refresh(),j.window.showInformationMessage("Entry updated successfully")}async requestEdit(e){let n=(await new q(this._context).getHistory()).find(a=>a.id===e);if(!n){j.window.showErrorMessage("Entry not found");return}let i=await j.window.showInputBox({prompt:"Enter tags (comma separated)",value:n.tags||"",placeHolder:"tag1, tag2, tag3"});if(i===void 0)return;let o=await j.window.showInputBox({prompt:"Enter description",value:n.description||"",placeHolder:"Description for this query"});o!==void 0&&await this.updateEntry(e,i,o)}async requestTagFilter(e){if(e.length===1)await this.sendFilteredByTagToWebview(e[0]);else if(e.length>1){let t=await j.window.showQuickPick(e,{placeHolder:"Filter by which tag?"});t&&await this.sendFilteredByTagToWebview(t)}}async sendFavoritesToWebview(){if(!this._view)return;let e=new q(this._context),t=await e.getFavorites(),s=await e.getStats();this._view.webview.postMessage({type:"historyData",history:t,stats:s,filter:"favorites"})}async sendFilteredByTagToWebview(e){if(!this._view)return;let t=new q(this._context),s=await t.getByTag(e),n=await t.getStats();this._view.webview.postMessage({type:"historyData",history:s,stats:n,filter:`tag: ${e}`})}_getHtmlForWebview(e){let t=It();return`<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src ${e.cspSource} 'unsafe-inline'; script-src 'nonce-${t}';">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Query History</title>
    <style>
        body {
            padding: 0;
            margin: 0;
            font-family: var(--vscode-font-family);
            font-size: var(--vscode-font-size);
            color: var(--vscode-foreground);
            background-color: var(--vscode-editor-background);
        }

        .toolbar {
            display: flex;
            flex-direction: column;
            gap: 8px;
            padding: 8px;
            border-bottom: 1px solid var(--vscode-panel-border);
            background-color: var(--vscode-sideBar-background);
        }

        .toolbar-top {
            display: flex;
            align-items: center;
            gap: 8px;
            width: 100%;
        }

        .toolbar-buttons {
            display: flex;
            gap: 3px;
            flex-wrap: wrap;
        }
        
        .toolbar-buttons button {
            padding: 2px 5px;
            font-size: 10px;
            line-height: 14px;
        }

        .stats {
            font-size: 11px;
            color: var(--vscode-descriptionForeground);
        }

        input[type="search"] {
            background: var(--vscode-input-background);
            color: var(--vscode-input-foreground);
            border: 1px solid var(--vscode-input-border);
            padding: 4px 8px;
            font-size: 12px;
            flex: 1;
            min-width: 150px;
        }

        button {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            gap: 4px;
            background-color: var(--vscode-button-secondaryBackground);
            color: var(--vscode-button-secondaryForeground);
            border: 1px solid var(--vscode-contrastBorder, transparent);
            padding: 2px 6px;
            cursor: pointer;
            border-radius: 2px;
            font-family: var(--vscode-font-family);
            font-size: 11px;
            line-height: 16px;
        }

        button:hover {
            background-color: var(--vscode-button-secondaryHoverBackground);
        }

        /* If we need a primary button override */
        button.primary {
            background-color: var(--vscode-button-background);
            color: var(--vscode-button-foreground);
        }

        button.primary:hover {
            background-color: var(--vscode-button-hoverBackground);
        }

        .history-container {
            overflow-y: auto;
            height: calc(100vh - 50px);
        }

        .history-item {
            padding: 12px;
            border-bottom: 1px solid var(--vscode-panel-border);
            cursor: pointer;
        }

        .history-item:hover {
            background-color: var(--vscode-list-hoverBackground);
        }

        .history-item:hover .action-btn {
            opacity: 0.7;
        }

        .history-item-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 6px;
        }

        .history-item-time {
            font-size: 11px;
            color: var(--vscode-descriptionForeground);
        }

        .history-item-actions {
            display: flex;
            gap: 4px;
        }

        .action-btn {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            padding: 2px 6px;
            font-size: 11px;
            background-color: transparent;
            color: var(--vscode-foreground);
            border: none;
            border-radius: 2px;
            cursor: pointer;
            font-family: var(--vscode-font-family);
            opacity: 0.8;
            transition: opacity 0.2s, background-color 0.2s;
        }

        .action-btn:hover {
            opacity: 1;
            background-color: var(--vscode-button-secondaryBackground);
        }

        .action-btn.delete {
            /* Optional: keep it same as others, or give it a slight redness on hover? */
            /* For consistency, let's keep it same, maybe just specific hover if needed. */
            /* Using standard secondary colors for now to match request "not fitting". */
        }

        .action-btn.delete:hover {
            background-color: var(--vscode-button-destructiveHoverBackground);
            color: var(--vscode-button-destructiveForeground);
        }

        .action-btn.favorite {
            color: gold;
            opacity: 1;
        }

        .tags {
            background: var(--vscode-badge-background);
            color: var(--vscode-badge-foreground);
            padding: 2px 6px;
            border-radius: 10px;
            font-size: 10px;
            cursor: pointer;
        }

        .history-item-description {
            font-size: 11px;
            color: var(--vscode-descriptionForeground);
            margin: 4px 0;
            font-style: italic;
        }

        .history-item-meta {
            font-size: 12px;
            color: var(--vscode-descriptionForeground);
            margin-bottom: 4px;
        }

        .history-item-meta span {
            margin-right: 12px;
        }

        .history-item-query {
            font-family: var(--vscode-editor-font-family);
            font-size: 12px;
            background: var(--vscode-textCodeBlock-background);
            padding: 6px;
            border-radius: 3px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }

        .empty-state {
            text-align: center;
            padding: 40px 20px;
            color: var(--vscode-descriptionForeground);
        }

        .empty-state-icon {
            font-size: 48px;
            margin-bottom: 16px;
        }
    </style>
</head>
<body>
    <div class="toolbar">
        <div class="toolbar-top">
            <input type="search" id="searchInput" placeholder="Search queries..." />
            <span class="stats" id="stats">Loading...</span>
        </div>
        <div class="toolbar-buttons">
            <button class="secondary" id="showAllBtn">\u{1F4DC} All</button>
            <button class="secondary" id="showFavoritesBtn">\u2B50 Favorites</button>
            <button class="secondary" id="refreshBtn">\u21BB Refresh</button>
            <button class="secondary" id="clearAllBtn">\u{1F5D1}\uFE0F Clear All</button>
        </div>
    </div>
    <div class="history-container" id="historyContainer">
        <div class="empty-state">
            <div class="empty-state-icon">\u{1F4DC}</div>
            <div>No query history yet</div>
        </div>
    </div>

    <script nonce="${t}">
        const vscode = acquireVsCodeApi();
        let allHistory = [];

        // Request history on load
        window.addEventListener('load', () => {
            console.log('queryHistory webview: load -> requesting history');
            vscode.postMessage({ type: 'getHistory' });
        });

        // Listen for messages from extension
        window.addEventListener('message', event => {
            const message = event.data;
            console.log('queryHistory webview: received message', message);
            switch (message.type) {
                case 'historyData':
                    allHistory = message.history;
                    updateStats(message.stats);
                    renderHistory(allHistory);
                    break;
                case 'debug':
                    console.log('queryHistory debug:', message.msg, message);
                    break;
            }
        });

        // Search functionality
        document.getElementById('searchInput').addEventListener('input', (e) => {
            const searchTerm = e.target.value.toLowerCase();
            if (!searchTerm) {
                renderHistory(allHistory);
                return;
            }

            const filtered = allHistory.filter(entry => 
                entry.query.toLowerCase().includes(searchTerm) ||
                entry.host.toLowerCase().includes(searchTerm) ||
                entry.database.toLowerCase().includes(searchTerm) ||
                entry.schema.toLowerCase().includes(searchTerm)
            );
            renderHistory(filtered);
        });

        function updateStats(stats) {
            const statsEl = document.getElementById('stats');
            statsEl.textContent = \`\${stats.totalEntries} entries \xB7 \${stats.totalFileSizeMB} MB\`;
        }

        function renderHistory(history) {
            const container = document.getElementById('historyContainer');
            
            if (history.length === 0) {
                container.innerHTML = \`
                    <div class="empty-state">
                        <div class="empty-state-icon">\u{1F4DC}</div>
                        <div>No query history found</div>
                    </div>
                \`;
                return;
            }

            container.innerHTML = history.map(entry => \`
                <div class="history-item">
                    <div class="history-item-header">
                        <div class="history-item-time">\${formatTimestamp(entry.timestamp)}</div>
                        <div class="history-item-actions">
                            <button class="action-btn \${entry.is_favorite ? 'favorite' : ''}" data-action="favorite" data-id="\${escapeHtml(entry.id)}">\${entry.is_favorite ? '\u2B50' : '\u2606'}</button>
                            <button class="action-btn" data-action="edit" data-id="\${escapeHtml(entry.id)}">\u270F\uFE0F</button>
                            <button class="action-btn" data-action="execute" data-id="\${escapeHtml(entry.id)}">\u25B6\uFE0F Run</button>
                            <button class="action-btn" data-action="copy" data-id="\${escapeHtml(entry.id)}">\u{1F4CB} Copy</button>
                            <button class="action-btn delete" data-action="delete" data-id="\${escapeHtml(entry.id)}">\u{1F5D1}\uFE0F</button>
                        </div>
                    </div>
                    <div class="history-item-meta">
                        <span>\u{1F5A5}\uFE0F \${escapeHtml(entry.host)}</span>
                        <span>\u{1F5C3}\uFE0F \${escapeHtml(entry.database)}</span>
                        <span>\u{1F4C1} \${escapeHtml(entry.schema)}</span>
                        \${entry.tags ? \`<span class="tags">\u{1F3F7}\uFE0F \${escapeHtml(entry.tags)}</span>\` : ''}
                    </div>
                    \${entry.description ? \`<div class="history-item-description">\${escapeHtml(entry.description)}</div>\` : ''}
                    <div class="history-item-query" title="\${escapeHtml(entry.query)}">\${escapeHtml(entry.query)}</div>
                </div>
            \`).join('');
        }

        function formatTimestamp(timestamp) {
            const date = new Date(timestamp);
            return date.toLocaleString();
        }

        function escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }

        function refreshHistory() {
            vscode.postMessage({ type: 'getHistory' });
        }

        function clearAllHistory() {
            vscode.postMessage({ type: 'clearAll' });
        }

        function deleteEntry(id) {
            vscode.postMessage({ type: 'deleteEntry', id: id });
        }

        function copyQuery(id) {
            const entry = allHistory.find(e => e.id === id);
            if (entry) {
                vscode.postMessage({ type: 'copyQuery', query: entry.query });
            }
        }

        function executeQuery(id) {
            const entry = allHistory.find(e => e.id === id);
            if (entry) {
                vscode.postMessage({ type: 'executeQuery', query: entry.query });
            }
        }

        function showFavorites() {
            vscode.postMessage({ type: 'showFavoritesOnly' });
        }

        function showAll() {
            vscode.postMessage({ type: 'getHistory' });
        }

        function toggleFavorite(id) {
            vscode.postMessage({ type: 'toggleFavorite', id: id });
        }

        function editEntry(id) {
            const entry = allHistory.find(e => e.id === id);
            if (entry) {
                vscode.postMessage({ 
                    type: 'requestEdit', 
                    id: id
                });
            }
        }

        function filterByTag(tag) {
            vscode.postMessage({ type: 'filterByTag', tag: tag });
        }

        // Attach event listeners (no inline handlers to satisfy CSP)
        window.addEventListener('load', () => {
            const refreshBtn = document.getElementById('refreshBtn');
            const clearBtn = document.getElementById('clearAllBtn');
            const showAllBtn = document.getElementById('showAllBtn');
            const showFavoritesBtn = document.getElementById('showFavoritesBtn');
            const container = document.getElementById('historyContainer');

            if (refreshBtn) {
                refreshBtn.addEventListener('click', (e) => { e.preventDefault(); refreshHistory(); });
            }
            if (clearBtn) {
                clearBtn.addEventListener('click', (e) => { e.preventDefault(); clearAllHistory(); });
            }
            if (showAllBtn) {
                showAllBtn.addEventListener('click', (e) => { e.preventDefault(); showAll(); });
            }
            if (showFavoritesBtn) {
                showFavoritesBtn.addEventListener('click', (e) => { e.preventDefault(); showFavorites(); });
            }

            if (container) {
                container.addEventListener('click', (e) => {
                    let target = e.target;
                    // Handle text nodes (e.g. clicking on the emoji)
                    if (!(target instanceof Element)) {
                        target = target.parentElement;
                    }
                    if (!target) return;
                    
                    const btn = target.closest('button');
                    if (!btn) return;
                    const action = btn.getAttribute('data-action');
                    const id = btn.getAttribute('data-id');
                    if (!action || !id) return;

                    if (action === 'execute') {
                        executeQuery(id);
                    } else if (action === 'copy') {
                        copyQuery(id);
                    } else if (action === 'delete') {
                        // Find entry to pass query text for confirmation
                        const entry = allHistory.find(e => e.id === id);
                        if (entry) {
                            vscode.postMessage({ type: 'deleteEntry', id: id, query: entry.query });
                        }
                    } else if (action === 'favorite') {
                        toggleFavorite(id);
                    } else if (action === 'edit') {
                        editEntry(id);
                    }
                });

                // Handle tag clicks
                container.addEventListener('click', (e) => {
                    const tagElement = e.target.closest('.tags');
                    if (tagElement) {
                        const tagText = tagElement.textContent.replace('\u{1F3F7}\uFE0F ', '').trim();
                        const tags = tagText.split(',').map(t => t.trim());
                        vscode.postMessage({ 
                            type: 'requestTagFilter', 
                            tags: tags 
                        });
                    }
                });
            }
        });
    </script>
</body>
</html>`}};function It(){let l="",e="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";for(let t=0;t<32;t++)l+=e.charAt(Math.floor(Math.random()*e.length));return l}var yt=$(require("path"));function Et(l,e){let t=e.getKeepConnectionOpen();l.text=t?"\u{1F517} Keep Connection ON":"\u{1F6AB} Keep Connection OFF",l.tooltip=t?"Keep Connection Open: ENABLED - Kliknij aby wy\u0142\u0105czy\u0107":"Keep Connection Open: DISABLED - Kliknij aby w\u0142\u0105czy\u0107",l.backgroundColor=t?new c.ThemeColor("statusBarItem.prominentBackground"):void 0}function Jt(l){console.log("Netezza extension: Activating..."),l.subscriptions.push({dispose:()=>{e.closePersistentConnection()}});let e=new oe(l),t=new ge(l,e),s=new me(l.extensionUri),n=c.window.createStatusBarItem(c.StatusBarAlignment.Right,100);n.command="netezza.toggleKeepConnectionOpen",Et(n,e),n.show(),l.subscriptions.push(n),console.log("Netezza extension: Registering SchemaSearchProvider...");let i=new he(l.extensionUri,l);console.log("Netezza extension: Registering QueryHistoryView...");let o=new fe(l.extensionUri,l),a=c.window.createTreeView("netezza.schema",{treeDataProvider:t,showCollapseAll:!0});l.subscriptions.push(c.window.registerWebviewViewProvider(me.viewType,s),c.window.registerWebviewViewProvider(he.viewType,i),c.window.registerWebviewViewProvider(fe.viewType,o));let r=/(^|\s)(?:[A-Za-z]:\\|\\|\/)?[\w.\-\\\/]+\.py\b|(^|\s)python(?:\.exe)?\s+[^\n]*\.py\b/i;function p(d){return d&&(d.includes(" ")?`"${d.replace(/"/g,'\\"')}"`:d)}function u(d,h,E){let v=/[ \\/]/.test(d)?`& ${p(d)}`:d,b=p(h),C=E.map(w=>p(w)).join(" ");return`${v} ${b}${C?" "+C:""}`.trim()}class m{constructor(){this._onDidChange=new c.EventEmitter;this.onDidChangeCodeLenses=this._onDidChange.event}provideCodeLenses(h){let E=[];for(let T=0;T<h.lineCount;T++){let v=h.lineAt(T);if(r.test(v.text)){let b=v.range,C={title:"Run as script",command:"netezza.runScriptFromLens",arguments:[h.uri,b]};E.push(new c.CodeLens(b,C))}}return E}refresh(){this._onDidChange.fire()}}let f=new m;l.subscriptions.push(c.languages.registerCodeLensProvider({scheme:"file"},f));let g=c.window.createTextEditorDecorationType({backgroundColor:new c.ThemeColor("editor.rangeHighlightBackground"),borderRadius:"3px"});function y(d){let h=d||c.window.activeTextEditor;if(!h)return;let E=h.document,T=[];for(let v=0;v<E.lineCount;v++){let b=E.lineAt(v);r.test(b.text)&&T.push({range:b.range,hoverMessage:"Python script invocation"})}h.setDecorations(g,T)}l.subscriptions.push(c.window.onDidChangeActiveTextEditor(()=>y()),c.workspace.onDidChangeTextDocument(d=>{c.window.activeTextEditor&&d.document===c.window.activeTextEditor.document&&y(c.window.activeTextEditor)})),l.subscriptions.push(c.commands.registerCommand("netezza.runScriptFromLens",async(d,h)=>{try{let E=await c.workspace.openTextDocument(d),T=E.getText(h).trim()||E.lineAt(h.start.line).text.trim();if(!T){c.window.showWarningMessage("No script command found");return}let v=T.split(/\s+/),b=v[0]||"",C=/python(\\.exe)?$/i.test(b)&&v.length>=2&&v[1].toLowerCase().endsWith(".py"),w=b.toLowerCase().endsWith(".py"),I=c.workspace.getConfiguration("netezza").get("pythonPath")||"python",x="";if(C){let _=v[0],W=v[1],V=v.slice(2);x=u(_,W,V)}else if(w){let _=b,W=v.slice(1);x=u(I,_,W)}else x=u(I,"",v);let B=c.window.createTerminal({name:"Netezza: Script"});B.show(!0),B.sendText(x,!0),c.window.showInformationMessage(`Running script: ${x}`)}catch(E){c.window.showErrorMessage(`Error running script: ${E.message}`)}})),y(c.window.activeTextEditor),l.subscriptions.push(c.window.onDidChangeActiveTextEditor(d=>{if(d&&d.document){let h=d.document.uri.toString();s.setActiveSource(h)}})),l.subscriptions.push(c.commands.registerCommand("netezza.toggleKeepConnectionOpen",()=>{let d=e.getKeepConnectionOpen();e.setKeepConnectionOpen(!d),Et(n,e);let h=!d;c.window.showInformationMessage(h?"Keep connection open: ENABLED - Po\u0142\u0105czenie b\u0119dzie utrzymane mi\u0119dzy zapytaniami":"Keep connection open: DISABLED - Po\u0142\u0105czenie b\u0119dzie zamykane po ka\u017Cdym zapytaniu")}),c.commands.registerCommand("netezza.openLogin",()=>{Ie.createOrShow(l.extensionUri,e)}),c.commands.registerCommand("netezza.refreshSchema",()=>{t.refresh(),c.window.showInformationMessage("Schema refreshed")}),c.commands.registerCommand("netezza.copySelectAll",d=>{if(d&&d.label&&d.dbName&&d.schema){let h=`SELECT * FROM ${d.dbName}.${d.schema}.${d.label} LIMIT 100;`;c.env.clipboard.writeText(h),c.window.showInformationMessage("Copied to clipboard")}}),c.commands.registerCommand("netezza.copyDrop",async d=>{if(d&&d.label&&d.dbName&&d.schema&&d.objType){let h=`${d.dbName}.${d.schema}.${d.label}`,E=`DROP ${d.objType} ${h};`;if(await c.window.showWarningMessage(`Czy na pewno chcesz usun\u0105\u0107 ${d.objType.toLowerCase()} "${h}"?`,{modal:!0},"Tak, usu\u0144","Anuluj")==="Tak, usu\u0144")try{if(!await e.getConnectionString()){c.window.showErrorMessage("Brak po\u0142\u0105czenia z baz\u0105 danych");return}await c.window.withProgress({location:c.ProgressLocation.Notification,title:`Usuwanie ${d.objType.toLowerCase()} ${h}...`,cancellable:!1},async b=>{await L(l,E,!0)}),c.window.showInformationMessage(`Usuni\u0119to ${d.objType.toLowerCase()}: ${h}`),t.refresh()}catch(v){c.window.showErrorMessage(`B\u0142\u0105d podczas usuwania: ${v.message}`)}}}),c.commands.registerCommand("netezza.copyName",d=>{if(d&&d.label&&d.dbName&&d.schema){let h=`${d.dbName}.${d.schema}.${d.label}`;c.env.clipboard.writeText(h),c.window.showInformationMessage("Copied to clipboard")}}),c.commands.registerCommand("netezza.grantPermissions",async d=>{if(d&&d.label&&d.dbName&&d.schema&&d.objType){let h=`${d.dbName}.${d.schema}.${d.label}`,E=await c.window.showQuickPick([{label:"SELECT",description:"Uprawnienia do odczytu danych"},{label:"INSERT",description:"Uprawnienia do wstawiania danych"},{label:"UPDATE",description:"Uprawnienia do aktualizacji danych"},{label:"DELETE",description:"Uprawnienia do usuwania danych"},{label:"ALL",description:"Wszystkie uprawnienia (SELECT, INSERT, UPDATE, DELETE)"},{label:"LIST",description:"Uprawnienia do listowania obiekt\xF3w"}],{placeHolder:"Wybierz typ uprawnie\u0144"});if(!E)return;let T=await c.window.showInputBox({prompt:"Podaj nazw\u0119 u\u017Cytkownika lub grupy",placeHolder:"np. SOME_USER lub GROUP_NAME",validateInput:C=>!C||C.trim().length===0?"Nazwa u\u017Cytkownika/grupy nie mo\u017Ce by\u0107 pusta":/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(C.trim())?null:"Nieprawid\u0142owa nazwa u\u017Cytkownika/grupy"});if(!T)return;let v=`GRANT ${E.label} ON ${h} TO ${T.trim().toUpperCase()};`;if(await c.window.showInformationMessage(`Wykona\u0107: ${v}`,{modal:!0},"Tak, wykonaj","Anuluj")==="Tak, wykonaj")try{if(!await e.getConnectionString()){c.window.showErrorMessage("Brak po\u0142\u0105czenia z baz\u0105 danych");return}await c.window.withProgress({location:c.ProgressLocation.Notification,title:`Nadawanie uprawnie\u0144 ${E.label} na ${h}...`,cancellable:!1},async w=>{await L(l,v,!0)}),c.window.showInformationMessage(`Nadano uprawnienia ${E.label} na ${h} dla ${T.trim().toUpperCase()}`)}catch(C){c.window.showErrorMessage(`B\u0142\u0105d podczas nadawania uprawnie\u0144: ${C.message}`)}}}),c.commands.registerCommand("netezza.groomTable",async d=>{if(d&&d.label&&d.dbName&&d.schema&&d.objType==="TABLE"){let h=`${d.dbName}.${d.schema}.${d.label}`,E=await c.window.showQuickPick([{label:"RECORDS ALL",description:"Groom all records (reclaim space from deleted rows)"},{label:"RECORDS READY",description:"Groom only ready records"},{label:"PAGES ALL",description:"Groom all pages (reorganize data pages)"},{label:"PAGES START",description:"Groom pages from start"},{label:"VERSIONS",description:"Groom versions (clean up old row versions)"}],{placeHolder:"Wybierz tryb GROOM"});if(!E)return;let T=await c.window.showQuickPick([{label:"DEFAULT",description:"Use default backupset",value:"DEFAULT"},{label:"NONE",description:"No backupset",value:"NONE"},{label:"Custom",description:"Specify custom backupset ID",value:"CUSTOM"}],{placeHolder:"Wybierz opcj\u0119 RECLAIM BACKUPSET"});if(!T)return;let v=T.value;if(T.value==="CUSTOM"){let w=await c.window.showInputBox({prompt:"Podaj ID backupset",placeHolder:"np. 12345",validateInput:A=>!A||A.trim().length===0?"ID backupset nie mo\u017Ce by\u0107 puste":/^\d+$/.test(A.trim())?null:"ID backupset musi by\u0107 liczb\u0105"});if(!w)return;v=w.trim()}let b=`GROOM TABLE ${h} ${E.label} RECLAIM BACKUPSET ${v};`;if(await c.window.showWarningMessage(`Wykona\u0107 GROOM na tabeli "${h}"?

${b}

Uwaga: Operacja mo\u017Ce by\u0107 czasoch\u0142onna dla du\u017Cych tabel.`,{modal:!0},"Tak, wykonaj","Anuluj")==="Tak, wykonaj")try{if(!await e.getConnectionString()){c.window.showErrorMessage("Brak po\u0142\u0105czenia z baz\u0105 danych");return}let A=Date.now();await c.window.withProgress({location:c.ProgressLocation.Notification,title:`GROOM TABLE ${h} (${E.label})...`,cancellable:!1},async x=>{await L(l,b,!0)});let I=((Date.now()-A)/1e3).toFixed(1);c.window.showInformationMessage(`GROOM zako\u0144czony pomy\u015Blnie (${I}s): ${h}`)}catch(w){c.window.showErrorMessage(`B\u0142\u0105d podczas GROOM: ${w.message}`)}}}),c.commands.registerCommand("netezza.addTableComment",async d=>{if(d&&d.label&&d.dbName&&d.schema&&d.objType==="TABLE"){let h=`${d.dbName}.${d.schema}.${d.label}`,E=await c.window.showInputBox({prompt:"Podaj komentarz do tabeli",placeHolder:"np. Tabela zawiera dane klient\xF3w",value:d.objectDescription||""});if(E===void 0)return;let T=`COMMENT ON TABLE ${h} IS '${E.replace(/'/g,"''")}';`;try{if(!await e.getConnectionString()){c.window.showErrorMessage("Brak po\u0142\u0105czenia z baz\u0105 danych");return}await L(l,T,!0),c.window.showInformationMessage(`Dodano komentarz do tabeli: ${h}`),t.refresh()}catch(v){c.window.showErrorMessage(`B\u0142\u0105d podczas dodawania komentarza: ${v.message}`)}}}),c.commands.registerCommand("netezza.generateStatistics",async d=>{if(d&&d.label&&d.dbName&&d.schema&&d.objType==="TABLE"){let h=`${d.dbName}.${d.schema}.${d.label}`,E=`GENERATE EXPRESS STATISTICS ON ${h};`;if(await c.window.showInformationMessage(`Wygenerowa\u0107 statystyki dla tabeli "${h}"?

${E}`,{modal:!0},"Tak, generuj","Anuluj")==="Tak, generuj")try{if(!await e.getConnectionString()){c.window.showErrorMessage("Brak po\u0142\u0105czenia z baz\u0105 danych");return}let b=Date.now();await c.window.withProgress({location:c.ProgressLocation.Notification,title:`Generowanie statystyk dla ${h}...`,cancellable:!1},async w=>{await L(l,E,!0)});let C=((Date.now()-b)/1e3).toFixed(1);c.window.showInformationMessage(`Statystyki wygenerowane pomy\u015Blnie (${C}s): ${h}`)}catch(v){c.window.showErrorMessage(`B\u0142\u0105d podczas generowania statystyk: ${v.message}`)}}}),c.commands.registerCommand("netezza.truncateTable",async d=>{if(d&&d.label&&d.dbName&&d.schema&&d.objType==="TABLE"){let h=`${d.dbName}.${d.schema}.${d.label}`,E=`TRUNCATE TABLE ${h};`;if(await c.window.showWarningMessage(`\u26A0\uFE0F UWAGA: Czy na pewno chcesz usun\u0105\u0107 WSZYSTKIE dane z tabeli "${h}"?

${E}

Ta operacja jest NIEODWRACALNA!`,{modal:!0},"Tak, usu\u0144 wszystkie dane","Anuluj")==="Tak, usu\u0144 wszystkie dane")try{if(!await e.getConnectionString()){c.window.showErrorMessage("Brak po\u0142\u0105czenia z baz\u0105 danych");return}await c.window.withProgress({location:c.ProgressLocation.Notification,title:`Czyszczenie tabeli ${h}...`,cancellable:!1},async b=>{await L(l,E,!0)}),c.window.showInformationMessage(`Tabela wyczyszczona: ${h}`)}catch(v){c.window.showErrorMessage(`B\u0142\u0105d podczas czyszczenia tabeli: ${v.message}`)}}}),c.commands.registerCommand("netezza.addPrimaryKey",async d=>{if(d&&d.label&&d.dbName&&d.schema&&d.objType==="TABLE"){let h=`${d.dbName}.${d.schema}.${d.label}`,E=await c.window.showInputBox({prompt:"Podaj nazw\u0119 klucza g\u0142\xF3wnego (constraint)",placeHolder:`np. PK_${d.label}`,value:`PK_${d.label}`,validateInput:w=>!w||w.trim().length===0?"Nazwa constraint nie mo\u017Ce by\u0107 pusta":/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(w.trim())?null:"Nieprawid\u0142owa nazwa constraint"});if(!E)return;let T=await c.window.showInputBox({prompt:"Podaj nazwy kolumn klucza g\u0142\xF3wnego (oddzielone przecinkami)",placeHolder:"np. COL1, COL2 lub ID",validateInput:w=>!w||w.trim().length===0?"Musisz poda\u0107 przynajmniej jedn\u0105 kolumn\u0119":null});if(!T)return;let v=T.split(",").map(w=>w.trim().toUpperCase()).join(", "),b=`ALTER TABLE ${h} ADD CONSTRAINT ${E.trim().toUpperCase()} PRIMARY KEY (${v});`;if(await c.window.showInformationMessage(`Doda\u0107 klucz g\u0142\xF3wny do tabeli "${h}"?

${b}`,{modal:!0},"Tak, dodaj","Anuluj")==="Tak, dodaj")try{if(!await e.getConnectionString()){c.window.showErrorMessage("Brak po\u0142\u0105czenia z baz\u0105 danych");return}await c.window.withProgress({location:c.ProgressLocation.Notification,title:`Dodawanie klucza g\u0142\xF3wnego do ${h}...`,cancellable:!1},async A=>{await L(l,b,!0)}),c.window.showInformationMessage(`Klucz g\u0142\xF3wny dodany: ${E.trim().toUpperCase()}`),t.refresh()}catch(w){c.window.showErrorMessage(`B\u0142\u0105d podczas dodawania klucza g\u0142\xF3wnego: ${w.message}`)}}}),c.commands.registerCommand("netezza.createDDL",async d=>{try{if(!d||!d.label||!d.dbName||!d.schema||!d.objType){c.window.showErrorMessage("Invalid object selected for DDL generation");return}let h=await e.getConnectionString();if(!h){c.window.showErrorMessage("Connection not configured. Please connect via Netezza: Connect...");return}await c.window.withProgress({location:c.ProgressLocation.Notification,title:`Generating DDL for ${d.objType} ${d.label}...`,cancellable:!1},async()=>{let{generateDDL:E}=await Promise.resolve().then(()=>(lt(),ct)),T=await E(h,d.dbName,d.schema,d.label,d.objType);if(T.success&&T.ddlCode){let v=await c.window.showQuickPick([{label:"Open in Editor",description:"Open DDL code in a new editor",value:"editor"},{label:"Copy to Clipboard",description:"Copy DDL code to clipboard",value:"clipboard"}],{placeHolder:"How would you like to access the DDL code?"});if(v)if(v.value==="editor"){let b=await c.workspace.openTextDocument({content:T.ddlCode,language:"sql"});await c.window.showTextDocument(b),c.window.showInformationMessage(`DDL code generated for ${d.objType} ${d.label}`)}else v.value==="clipboard"&&(await c.env.clipboard.writeText(T.ddlCode),c.window.showInformationMessage("DDL code copied to clipboard"))}else throw new Error(T.error||"DDL generation failed")})}catch(h){c.window.showErrorMessage(`Error generating DDL: ${h.message}`)}}),c.commands.registerCommand("netezza.revealInSchema",async d=>{try{if(!await e.getConnectionString()){c.window.showWarningMessage("Not connected to database");return}let E=d.name,T=d.objType,v=T?[T]:["TABLE","VIEW","EXTERNAL TABLE","PROCEDURE","FUNCTION","SEQUENCE","SYNONYM"];if(T==="COLUMN"){if(!d.parent){c.window.showWarningMessage("Cannot find column without parent table");return}E=d.parent}let b=[];if(d.database)b=[d.database];else{let C=await L(l,"SELECT DATABASE FROM system.._v_database ORDER BY DATABASE",!0);if(!C)return;b=JSON.parse(C).map(A=>A.DATABASE)}for(let C of b)try{let w=T==="COLUMN"?["TABLE","VIEW","EXTERNAL TABLE"]:v;for(let A of w){let I=`SELECT OBJNAME, OBJTYPE, SCHEMA, OBJID FROM ${C}.._V_OBJECT_DATA WHERE UPPER(OBJNAME) = UPPER('${E.replace(/'/g,"''")}') AND UPPER(OBJTYPE) = UPPER('${A}') AND DBNAME = '${C}'`;d.schema&&(I+=` AND UPPER(SCHEMA) = UPPER('${d.schema.replace(/'/g,"''")}')`);let x=await L(l,I,!0);if(x){let B=JSON.parse(x);if(B.length>0){let _=B[0],{SchemaItem:W}=await Promise.resolve().then(()=>(Ue(),ot)),V=new W(_.OBJNAME,c.TreeItemCollapsibleState.Collapsed,`netezza:${_.OBJTYPE}`,C,_.OBJTYPE,_.SCHEMA,_.OBJID);await a.reveal(V,{select:!0,focus:!0,expand:!0});return}}}}catch(w){console.log(`Error searching in ${C}:`,w)}c.window.showWarningMessage(`Could not find ${T||"object"} ${E}`)}catch(h){c.window.showErrorMessage(`Error revealing item: ${h.message}`)}}),c.commands.registerCommand("netezza.showQueryHistory",()=>{c.commands.executeCommand("netezza.queryHistory.focus")}),c.commands.registerCommand("netezza.clearQueryHistory",async()=>{let{QueryHistoryManager:d}=await Promise.resolve().then(()=>(Ae(),Ye)),h=new d(l);await c.window.showWarningMessage("Are you sure you want to clear all query history?",{modal:!0},"Clear All")==="Clear All"&&(await h.clearHistory(),o.refresh(),c.window.showInformationMessage("Query history cleared"))})),l.subscriptions.push(c.languages.registerDocumentLinkProvider({language:"sql"},new De)),l.subscriptions.push(c.languages.registerFoldingRangeProvider({language:"sql"},new Me)),l.subscriptions.push(c.commands.registerCommand("netezza.jumpToSchema",async()=>{let d=c.window.activeTextEditor;if(!d)return;let h=d.document,E=d.selection,T=h.offsetAt(E.active),v=Z.getObjectAtPosition(h.getText(),T);v?c.commands.executeCommand("netezza.revealInSchema",v):c.window.showWarningMessage("No object found at cursor")}));let S=c.commands.registerCommand("netezza.runQuery",async()=>{let d=c.window.activeTextEditor;if(!d){c.window.showErrorMessage("No active editor found");return}let h=d.document,E=d.selection,T=h.getText(),v=h.uri.toString(),b=[];if(E.isEmpty){let w=h.offsetAt(E.active),A=Z.getStatementAtPosition(T,w);if(A){b=[A.sql];let I=h.positionAt(A.start),x=h.positionAt(A.end);d.selection=new c.Selection(I,x)}else{c.window.showWarningMessage("No SQL statement found at cursor");return}}else{let w=h.getText(E);if(!w.trim()){c.window.showWarningMessage("No SQL query selected");return}b=Z.splitStatements(w)}if(b.length===0)return;let C=b.length===1?b[0].trim():null;if(C){let w=C.split(/\s+/),A=w[0]||"",I=/python(\.exe)?$/i.test(A)&&w.length>=2&&w[1].toLowerCase().endsWith(".py"),x=A.toLowerCase().endsWith(".py");if(I||x){let _=c.workspace.getConfiguration("netezza").get("pythonPath")||"python",W="";if(I){let se=w[0],ue=w[1],vt=w.slice(2);W=u(se,ue,vt)}else{let se=A,ue=w.slice(1);W=u(_,se,ue)}let V=c.window.createTerminal({name:"Netezza: Script"});V.show(!0),V.sendText(W,!0),c.window.showInformationMessage(`Running script: ${W}`);return}}try{let w=await Be(l,b,e);s.updateResults(w,v,!1),c.commands.executeCommand("netezza.results.focus")}catch(w){c.window.showErrorMessage(`Error executing query: ${w.message}`)}}),R=c.commands.registerCommand("netezza.runQueryBatch",async()=>{let d=c.window.activeTextEditor;if(!d){c.window.showErrorMessage("No active editor found");return}let h=d.document,E=d.selection,T=h.uri.toString(),v;if(E.isEmpty?v=h.getText():v=h.getText(E),!v.trim()){c.window.showWarningMessage("No SQL query to execute");return}let C=v.trim().split(/\s+/),w=C[0]||"",A=/python(\.exe)?$/i.test(w)&&C.length>=2&&C[1].toLowerCase().endsWith(".py"),I=w.toLowerCase().endsWith(".py");if(A||I){let B=c.workspace.getConfiguration("netezza").get("pythonPath")||"python",_="";if(A){let V=C[0],se=C[1],ue=C.slice(2);_=u(V,se,ue)}else{let V=w,se=C.slice(1);_=u(B,V,se)}let W=c.window.createTerminal({name:"Netezza: Script"});W.show(!0),W.sendText(_,!0),c.window.showInformationMessage(`Running script: ${_}`);return}try{let{runQueryRaw:x}=await Promise.resolve().then(()=>(ae(),st)),B=await x(l,v,!1,e);B&&(s.updateResults([B],T,!1),c.commands.executeCommand("netezza.results.focus"))}catch(x){c.window.showErrorMessage(`Error executing query: ${x.message}`)}});l.subscriptions.push(R);let D=c.window.createOutputChannel("Netezza"),N=(d,h)=>{let E=Date.now()-h;D.appendLine(`[${new Date().toLocaleTimeString()}] ${d} completed in ${E}ms`),D.show(!0)},M=c.commands.registerCommand("netezza.exportToXlsb",async()=>{let d=c.window.activeTextEditor;if(!d){c.window.showErrorMessage("No active editor found");return}let h=d.selection,E=h.isEmpty?d.document.getText():d.document.getText(h);if(!E.trim()){c.window.showWarningMessage("No SQL query to export");return}let T=await c.window.showSaveDialog({filters:{"Excel Workbook":["xlsx"]},saveLabel:"Export to XLSX"});if(!T)return;let v=Date.now();try{let b=await e.getConnectionString();if(!b)throw new Error("Connection not configured. Please connect via Netezza: Connect...");await c.window.withProgress({location:c.ProgressLocation.Notification,title:"Exporting to XLSX...",cancellable:!1},async C=>{let{exportQueryToXlsb:w}=await Promise.resolve().then(()=>(Ee(),we)),A=await w(b,E,T.fsPath,!1,I=>{C.report({message:I}),D.appendLine(`[XLSX Export] ${I}`)});if(!A.success)throw new Error(A.message)}),N("Export to XLSX",v),c.window.showInformationMessage(`Results exported to ${T.fsPath}`)}catch(b){c.window.showErrorMessage(`Error exporting to XLSX: ${b.message}`)}}),k=c.commands.registerCommand("netezza.exportToCsv",async()=>{let d=c.window.activeTextEditor;if(!d){c.window.showErrorMessage("No active editor found");return}let h=d.selection,E=h.isEmpty?d.document.getText():d.document.getText(h);if(!E.trim()){c.window.showWarningMessage("No SQL query to export");return}let T=await c.window.showSaveDialog({filters:{"CSV Files":["csv"]},saveLabel:"Export to CSV"});if(!T)return;let v=Date.now();try{let b=await e.getConnectionString();if(!b)throw new Error("Connection not configured. Please connect via Netezza: Connect...");await c.window.withProgress({location:c.ProgressLocation.Notification,title:"Exporting to CSV...",cancellable:!1},async C=>{let{exportToCsv:w}=await Promise.resolve().then(()=>(gt(),pt));await w(l,b,E,T.fsPath,C)}),N("Export to CSV",v),c.window.showInformationMessage(`Results exported to ${T.fsPath}`)}catch(b){c.window.showErrorMessage(`Error exporting to CSV: ${b.message}`)}}),te=c.commands.registerCommand("netezza.copyXlsbToClipboard",async()=>{let d=c.window.activeTextEditor;if(!d){c.window.showErrorMessage("No active editor found");return}let h=d.selection,E=h.isEmpty?d.document.getText():d.document.getText(h);if(!E.trim()){c.window.showWarningMessage("No SQL query to export");return}try{let T=await e.getConnectionString();if(!T)throw new Error("Connection not configured. Please connect via Netezza: Connect...");let v=Date.now();if(await c.window.withProgress({location:c.ProgressLocation.Notification,title:"Exporting to XLSX and copying to clipboard...",cancellable:!1},async C=>{let{exportQueryToXlsb:w,getTempFilePath:A}=await Promise.resolve().then(()=>(Ee(),we)),I=A(),x=await w(T,E,I,!0,B=>{C.report({message:B}),D.appendLine(`[XLSX Clipboard] ${B}`)});if(!x.success)throw new Error(x.message);if(!x.details?.clipboard_success)throw new Error("Failed to copy file to clipboard")}),N("Copy XLSX to Clipboard",v),await c.window.showInformationMessage("Excel file copied to clipboard! You can now paste it into Excel or Windows Explorer.","Show Temp Folder","OK")==="Show Temp Folder"){let C=require("os").tmpdir();await c.env.openExternal(c.Uri.file(C))}}catch(T){c.window.showErrorMessage(`Error copying XLSX to clipboard: ${T.message}`)}}),ve=c.commands.registerCommand("netezza.exportToXlsbAndOpen",async()=>{let d=c.window.activeTextEditor;if(!d){c.window.showErrorMessage("No active editor found");return}let h=d.selection,E=h.isEmpty?d.document.getText():d.document.getText(h);if(!E.trim()){c.window.showWarningMessage("No SQL query to export");return}let T=await c.window.showSaveDialog({filters:{"Excel Workbook":["xlsx"]},saveLabel:"Export to XLSX and Open"});if(!T)return;let v=Date.now();try{let b=await e.getConnectionString();if(!b)throw new Error("Connection not configured. Please connect via Netezza: Connect...");await c.window.withProgress({location:c.ProgressLocation.Notification,title:"Exporting to XLSX and opening...",cancellable:!1},async C=>{let{exportQueryToXlsb:w}=await Promise.resolve().then(()=>(Ee(),we)),A=await w(b,E,T.fsPath,!1,I=>{C.report({message:I}),D.appendLine(`[XLSX Export] ${I}`)});if(!A.success)throw new Error(A.message)}),N("Export to XLSX and Open",v),await c.env.openExternal(T),c.window.showInformationMessage(`Results exported and opened: ${T.fsPath}`)}catch(b){c.window.showErrorMessage(`Error exporting to XLSX: ${b.message}`)}}),Pe=c.commands.registerCommand("netezza.importClipboard",async()=>{try{let d=await e.getConnectionString();if(!d)throw new Error("Connection not configured. Please connect via Netezza: Connect...");let h=await c.window.showInputBox({prompt:"Enter target table name (leave empty for auto-generated name)",placeHolder:"e.g. my_schema.my_table or leave empty",validateInput:b=>!b||b.trim().length===0||/^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?(\.[a-zA-Z_][a-zA-Z0-9_]*)?$/.test(b.trim())?null:"Invalid table name format. Use: [database.]schema.table"});if(h===void 0)return;let E;if(!h||h.trim().length===0)try{let C=await L(l,"SELECT CURRENT_CATALOG, CURRENT_SCHEMA",!0);if(C){let w=JSON.parse(C);if(w&&w.length>0){let A=w[0].CURRENT_CATALOG||"SYSTEM",I=w[0].CURRENT_SCHEMA||"ADMIN",B=new Date().toISOString().slice(0,10).replace(/-/g,""),_=Math.floor(Math.random()*1e4).toString().padStart(4,"0");E=`${A}.${I}.IMPORT_${B}_${_}`,c.window.showInformationMessage(`Auto-generated table name: ${E}`)}else throw new Error("Could not determine current database/schema")}else throw new Error("Could not determine current database/schema")}catch(b){c.window.showErrorMessage(`Error getting current database/schema: ${b.message}`);return}else E=h.trim();let T=await c.window.showQuickPick([{label:"Auto-detect",description:"Automatically detect clipboard format (text or Excel XML)",value:null},{label:"Excel XML Spreadsheet",description:"Force Excel XML format processing",value:"XML Spreadsheet"},{label:"Plain Text",description:"Force plain text processing with delimiter detection",value:"TEXT"}],{placeHolder:"Select clipboard data format"});if(!T)return;let v=Date.now();await c.window.withProgress({location:c.ProgressLocation.Notification,title:"Importing clipboard data...",cancellable:!1},async b=>{let{importClipboardDataToNetezza:C}=await Promise.resolve().then(()=>(wt(),ft)),w=await C(E,d,T.value,{},A=>{b.report({message:A}),D.appendLine(`[Clipboard Import] ${A}`)});if(!w.success)throw new Error(w.message);w.details&&(D.appendLine(`[Clipboard Import] Rows processed: ${w.details.rowsProcessed}`),D.appendLine(`[Clipboard Import] Columns: ${w.details.columns}`),D.appendLine(`[Clipboard Import] Format: ${w.details.format}`))}),N("Import Clipboard Data",v),c.window.showInformationMessage(`Clipboard data imported successfully to table: ${E}`)}catch(d){c.window.showErrorMessage(`Error importing clipboard data: ${d.message}`)}}),Te=c.commands.registerCommand("netezza.importData",async()=>{try{let d=await e.getConnectionString();if(!d)throw new Error("Connection not configured. Please connect via Netezza: Connect...");let h=await c.window.showOpenDialog({canSelectFiles:!0,canSelectFolders:!1,canSelectMany:!1,filters:{"Data Files":["csv","txt","xlsx","xlsb","json"],"CSV Files":["csv"],"Excel Files":["xlsx","xlsb"],"Text Files":["txt"],"JSON Files":["json"],"All Files":["*"]},openLabel:"Select file to import"});if(!h||h.length===0)return;let E=h[0].fsPath,T=await c.window.showInputBox({prompt:"Enter target table name (leave empty for auto-generated name)",placeHolder:"e.g. my_schema.my_table or leave empty",validateInput:w=>!w||w.trim().length===0||/^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?(\.[a-zA-Z_][a-zA-Z0-9_]*)?$/.test(w.trim())?null:"Invalid table name format. Use: [database.]schema.table"});if(T===void 0)return;let v;if(!T||T.trim().length===0)try{let A=await L(l,"SELECT CURRENT_CATALOG, CURRENT_SCHEMA",!0);if(A){let I=JSON.parse(A);if(I&&I.length>0){let x=I[0].CURRENT_CATALOG||"SYSTEM",B=I[0].CURRENT_SCHEMA||"ADMIN",W=new Date().toISOString().slice(0,10).replace(/-/g,""),V=Math.floor(Math.random()*1e4).toString().padStart(4,"0");v=`${x}.${B}.IMPORT_${W}_${V}`,c.window.showInformationMessage(`Auto-generated table name: ${v}`)}else throw new Error("Could not determine current database/schema")}else throw new Error("Could not determine current database/schema")}catch(w){c.window.showErrorMessage(`Error getting current database/schema: ${w.message}`);return}else v=T.trim();let b=await c.window.showQuickPick([{label:"Default Import",description:"Use default settings",value:{}},{label:"Custom Options",description:"Configure import settings (coming soon)",value:null}],{placeHolder:"Select import options"});if(!b)return;if(b.value===null){c.window.showInformationMessage("Custom options will be available in future version");return}let C=Date.now();await c.window.withProgress({location:c.ProgressLocation.Notification,title:"Importing data...",cancellable:!1},async w=>{let{importDataToNetezza:A}=await Promise.resolve().then(()=>(qe(),mt)),I=await A(E,v,d,b.value||{},x=>{w.report({message:x}),D.appendLine(`[Import] ${x}`)});if(!I.success)throw new Error(I.message);I.details&&(D.appendLine(`[Import] Rows processed: ${I.details.rowsProcessed}`),D.appendLine(`[Import] Columns: ${I.details.columns}`),D.appendLine(`[Import] Delimiter: ${I.details.detectedDelimiter}`))}),N("Import Data",C),c.window.showInformationMessage(`Data imported successfully to table: ${v}`)}catch(d){c.window.showErrorMessage(`Error importing data: ${d.message}`)}}),be=c.commands.registerCommand("netezza.exportCurrentResultToXlsbAndOpen",async d=>{try{if(!d){c.window.showErrorMessage("No data to export");return}let h=require("os"),E=require("path"),T=new Date().toISOString().replace(/[:.]/g,"-"),v=E.join(h.tmpdir(),`netezza_results_${T}.xlsx`),b=Date.now();await c.window.withProgress({location:c.ProgressLocation.Notification,title:"Creating Excel file...",cancellable:!1},async w=>{let{exportCsvToXlsb:A}=await Promise.resolve().then(()=>(Ee(),we)),I=await A(d,v,!1,{source:"Query Results Panel"},x=>{w.report({message:x}),D.appendLine(`[CSV to XLSX] ${x}`)});if(!I.success)throw new Error(I.message)});let C=Date.now()-b;D.appendLine(`[${new Date().toLocaleTimeString()}] Export Current Result to Excel completed in ${C}ms`),await c.env.openExternal(c.Uri.file(v)),c.window.showInformationMessage(`Results exported and opened: ${v}`)}catch(h){c.window.showErrorMessage(`Error exporting to Excel: ${h.message}`)}});l.subscriptions.push(S),l.subscriptions.push(M),l.subscriptions.push(k),l.subscriptions.push(te),l.subscriptions.push(ve),l.subscriptions.push(be),l.subscriptions.push(Pe),l.subscriptions.push(Te);let Je=c.workspace.onWillSaveTextDocument(async d=>{}),z=c.commands.registerCommand("netezza.smartPaste",async()=>{try{let d=c.window.activeTextEditor;if(!d)return;let E=c.workspace.getConfiguration("netezza").get("pythonPath")||"python",T=yt.join(l.extensionPath,"python","check_clipboard_format.py"),v=require("child_process");if(await new Promise(C=>{let w=v.spawn(E,[T]);w.on("close",A=>{C(A===1)}),w.on("error",()=>{C(!1)})})){let C=await c.window.showQuickPick([{label:"\u{1F4CA} Importuj do tabeli Netezza",description:'Wykryto format "XML Spreadsheet" - importuj dane do bazy',value:"import"},{label:"\u{1F4DD} Wklej jako tekst",description:"Wklej zawarto\u015B\u0107 schowka jako zwyk\u0142y tekst",value:"paste"}],{placeHolder:'Wykryto format "XML Spreadsheet" w schowku - wybierz akcj\u0119'});if(C?.value==="import")c.commands.executeCommand("netezza.importClipboard");else if(C?.value==="paste"){let w=await c.env.clipboard.readText(),A=d.selection;await d.edit(I=>{I.replace(A,w)})}}else{let C=await c.env.clipboard.readText(),w=d.selection;await d.edit(A=>{A.replace(w,C)})}}catch(d){c.window.showErrorMessage(`B\u0142\u0105d podczas wklejania: ${d.message}`)}}),ne=c.workspace.onDidChangeTextDocument(async d=>{if(d.document.languageId!=="sql"&&d.document.languageId!=="mssql"||d.contentChanges.length!==1)return;let h=d.contentChanges[0];if(h.text!==" ")return;let E=c.window.activeTextEditor;if(!E||E.document!==d.document)return;let v=d.document.lineAt(h.range.start.line).text,b=new Map([["SX","SELECT"],["WX","WHERE"],["GX","GROUP BY"],["HX","HAVING"],["OX","ORDER BY"],["FX","FROM"],["JX","JOIN"],["LX","LIMIT"],["IX","INSERT INTO"],["UX","UPDATE"],["DX","DELETE FROM"],["CX","CREATE TABLE"]]);for(let[C,w]of b)if(new RegExp(`\\b${C}\\s$`,"i").test(v)){let I=v.toUpperCase().lastIndexOf(C.toUpperCase());if(I>=0){let x=new c.Position(h.range.start.line,I),B=new c.Position(h.range.start.line,I+C.length+1);await E.edit(_=>{_.replace(new c.Range(x,B),w+" ")}),["SELECT","FROM","JOIN"].includes(w)&&setTimeout(()=>{c.commands.executeCommand("editor.action.triggerSuggest")},100);break}}});l.subscriptions.push(z),l.subscriptions.push(ne),l.subscriptions.push(c.languages.registerCompletionItemProvider(["sql","mssql"],new Ne(l),"."," "))}function Qt(){}0&&(module.exports={activate,deactivate});
