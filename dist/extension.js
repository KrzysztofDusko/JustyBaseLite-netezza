"use strict";var It=Object.create;var Ae=Object.defineProperty;var Dt=Object.getOwnPropertyDescriptor;var Mt=Object.getOwnPropertyNames;var xt=Object.getPrototypeOf,Ot=Object.prototype.hasOwnProperty;var ne=(l,e)=>()=>(l&&(e=l(l=0)),e);var se=(l,e)=>{for(var t in e)Ae(l,t,{get:e[t],enumerable:!0})},tt=(l,e,t,s)=>{if(e&&typeof e=="object"||typeof e=="function")for(let n of Mt(e))!Ot.call(l,n)&&n!==t&&Ae(l,n,{get:()=>e[n],enumerable:!(s=Dt(e,n))||s.enumerable});return l};var B=(l,e,t)=>(t=l!=null?It(xt(l)):{},tt(e||!l||!l.__esModule?Ae(t,"default",{value:l,enumerable:!0}):t,l)),$t=l=>tt(Ae({},"__esModule",{value:!0}),l);var Re,ue,Xe=ne(()=>{"use strict";Re=B(require("vscode")),ue=class l{constructor(e){this.context=e;this._connections={};this._activeConnectionName=null;this._documentConnections=new Map;this._persistentConnections=new Map;this._keepConnectionOpen=!1;this._onDidChangeConnections=new Re.EventEmitter;this.onDidChangeConnections=this._onDidChangeConnections.event;this._onDidChangeActiveConnection=new Re.EventEmitter;this.onDidChangeActiveConnection=this._onDidChangeActiveConnection.event;this._onDidChangeDocumentConnection=new Re.EventEmitter;this.onDidChangeDocumentConnection=this._onDidChangeDocumentConnection.event;this._loadingPromise=this.loadConnections()}static{this.SERVICE_NAME="netezza-vscode-connections"}static{this.ACTIVE_CONN_KEY="netezza-active-connection"}async loadConnections(){this._activeConnectionName=this.context.globalState.get(l.ACTIVE_CONN_KEY)||null;let e=await this.context.secrets.get(l.SERVICE_NAME);if(e)try{this._connections=JSON.parse(e)}catch(t){console.error("Failed to parse connections:",t),this._connections={}}else{let t=await this.context.secrets.get("netezza-vscode");if(t)try{let s=JSON.parse(t);if(s&&s.host){let n=`Default (${s.host})`;this._connections={[n]:{...s,name:n}},this._activeConnectionName=n,await this.saveConnectionsToStorage()}}catch{}}this._onDidChangeConnections.fire()}async ensureLoaded(){await this._loadingPromise}async saveConnectionsToStorage(){await this.context.secrets.store(l.SERVICE_NAME,JSON.stringify(this._connections)),this._activeConnectionName?await this.context.globalState.update(l.ACTIVE_CONN_KEY,this._activeConnectionName):await this.context.globalState.update(l.ACTIVE_CONN_KEY,void 0)}async saveConnection(e){if(await this.ensureLoaded(),!e.name)throw new Error("Connection name is required");this._connections[e.name]=e,this._activeConnectionName||await this.setActiveConnection(e.name),await this.saveConnectionsToStorage(),this._onDidChangeConnections.fire()}async deleteConnection(e){if(await this.ensureLoaded(),this._connections[e]){if(await this.closePersistentConnection(e),delete this._connections[e],this._activeConnectionName===e){let t=Object.keys(this._connections);await this.setActiveConnection(t.length>0?t[0]:null)}await this.saveConnectionsToStorage(),this._onDidChangeConnections.fire()}}async getConnections(){return await this.ensureLoaded(),Object.values(this._connections)}async getConnection(e){return await this.ensureLoaded(),this._connections[e]}async setActiveConnection(e){await this.ensureLoaded(),this._activeConnectionName=e,await this.context.globalState.update(l.ACTIVE_CONN_KEY,e),this._onDidChangeActiveConnection.fire(e)}getActiveConnectionName(){return this._activeConnectionName}async getConnectionString(e){await this.ensureLoaded();let t=e||this._activeConnectionName;if(!t)return null;let s=this._connections[t];return s?`DRIVER={NetezzaSQL};SERVER=${s.host};PORT=${s.port};DATABASE=${s.database};UID=${s.user};PWD=${s.password};`:(console.error(`[ConnectionManager] Connection '${t}' not found in registry. Available keys: ${Object.keys(this._connections).join(", ")}`),null)}async getCurrentDatabase(e){await this.ensureLoaded();let t=e||this._activeConnectionName;return t&&this._connections[t]?.database||null}setKeepConnectionOpen(e){this._keepConnectionOpen=e,e||this.closeAllPersistentConnections()}getKeepConnectionOpen(){return this._keepConnectionOpen}async getPersistentConnection(e){let t=e||this._activeConnectionName;if(!t)throw new Error("No connection selected");let s=await this.getConnectionString(t);if(!s)throw new Error(`Connection '${t}' not found or invalid`);let n=this._persistentConnections.get(t);return n||(n=await require("odbc").connect({connectionString:s,fetchArray:!0}),this._persistentConnections.set(t,n)),n}async closePersistentConnection(e){let t=this._persistentConnections.get(e);if(t){try{await t.close()}catch(s){console.error(`Error closing connection ${e}:`,s)}this._persistentConnections.delete(e)}}async closeAllPersistentConnections(){for(let e of this._persistentConnections.keys())await this.closePersistentConnection(e)}getDocumentConnection(e){return this._documentConnections.get(e)}setDocumentConnection(e,t){this._documentConnections.set(e,t),this._onDidChangeDocumentConnection.fire(e)}clearDocumentConnection(e){this._documentConnections.delete(e),this._onDidChangeDocumentConnection.fire(e)}getConnectionForExecution(e){if(e){let t=this._documentConnections.get(e);if(t)return t}return this._activeConnectionName||void 0}}});var nt={};se(nt,{QueryHistoryManager:()=>Q});var pe,Ne,Q,Ie=ne(()=>{"use strict";pe=B(require("fs")),Ne=B(require("path")),Q=class l{constructor(e){this.context=e;this.cache=[];this.initialized=!1;this.initialize()}static{this.STORAGE_KEY="queryHistory"}static{this.MAX_ENTRIES=5e4}static{this.CLEANUP_KEEP=4e4}static{this.STORAGE_VERSION=1}async initialize(){try{let e=this.context.globalState.get(l.STORAGE_KEY);e&&e.entries?(this.cache=e.entries,console.log(`\u2705 Loaded ${this.cache.length} entries from VS Code storage`)):await this.migrateFromLegacyStorage(),this.initialized=!0}catch(e){console.error("\u274C Error initializing query history:",e),this.cache=[],this.initialized=!0}}async migrateFromLegacyStorage(){try{let e=this.context.globalStorageUri.fsPath,t=Ne.join(e,"query-history.db");pe.existsSync(t)&&(console.log("\u26A0\uFE0F SQLite database found but migration not implemented"),console.log("\u{1F4A1} Consider manually exporting data before switching"));let s=Ne.join(e,"query-history.json");if(pe.existsSync(s)){let i=pe.readFileSync(s,"utf8");if(i.trim()){let a=JSON.parse(i);this.cache=a,await this.saveToStorage(),console.log(`\u2705 Migrated ${a.length} entries from JSON`)}}let n=Ne.join(e,"query-history-archive.json");if(pe.existsSync(n)){let i=pe.readFileSync(n,"utf8");if(i.trim()){let a=JSON.parse(i);this.cache.push(...a),await this.saveToStorage(),console.log(`\u2705 Migrated archive with ${a.length} entries`)}}}catch(e){console.error("Error during migration:",e)}}async saveToStorage(){try{let e={entries:this.cache,version:l.STORAGE_VERSION};await this.context.globalState.update(l.STORAGE_KEY,e)}catch(e){console.error("Error saving to storage:",e)}}async addEntry(e,t,s,n,i,a,o){try{this.initialized||await this.initialize();let r=`${Date.now()}-${Math.random().toString(36).substring(2,9)}`,p=Date.now(),u={id:r,host:e,database:t,schema:s,query:n.trim(),timestamp:p,connectionName:i,is_favorite:!1,tags:a||"",description:o||""};this.cache.unshift(u),this.cache.length>l.MAX_ENTRIES&&(this.cache=this.cache.slice(0,l.CLEANUP_KEEP),console.log(`Cleaned up old entries, keeping ${l.CLEANUP_KEEP} newest`)),await this.saveToStorage()}catch(r){console.error("Error adding query to history:",r)}}async getHistory(){return this.initialized||await this.initialize(),[...this.cache]}async deleteEntry(e){try{this.cache=this.cache.filter(t=>t.id!==e),await this.saveToStorage()}catch(t){console.error("Error deleting entry:",t)}}async clearHistory(){try{this.cache=[],await this.saveToStorage(),console.log("All query history cleared")}catch(e){console.error("Error clearing history:",e)}}async getStats(){try{let e=this.cache.length,t=JSON.stringify(this.cache).length,s=parseFloat((t/(1024*1024)).toFixed(2));return{activeEntries:e,archivedEntries:0,totalEntries:e,activeFileSizeMB:s,archiveFileSizeMB:0,totalFileSizeMB:s}}catch(e){return console.error("Error getting stats:",e),{activeEntries:0,archivedEntries:0,totalEntries:0,activeFileSizeMB:0,archiveFileSizeMB:0,totalFileSizeMB:0}}}async toggleFavorite(e){try{let t=this.cache.find(s=>s.id===e);t&&(t.is_favorite=!t.is_favorite,await this.saveToStorage())}catch(t){console.error("Error toggling favorite:",t)}}async updateEntry(e,t,s){try{let n=this.cache.find(i=>i.id===e);n&&(t!==void 0&&(n.tags=t),s!==void 0&&(n.description=s),await this.saveToStorage())}catch(n){console.error("Error updating entry:",n)}}async getFavorites(){return this.initialized||await this.initialize(),this.cache.filter(e=>e.is_favorite)}async getByTag(e){return this.initialized||await this.initialize(),this.cache.filter(t=>t.tags?.toLowerCase().includes(e.toLowerCase()))}async getAllTags(){this.initialized||await this.initialize();let e=new Set;return this.cache.forEach(t=>{t.tags&&t.tags.split(",").forEach(n=>{let i=n.trim();i&&e.add(i)})}),Array.from(e).sort()}async searchAll(e){this.initialized||await this.initialize();let t=e.toLowerCase();return this.cache.filter(s=>s.query.toLowerCase().includes(t)||s.host.toLowerCase().includes(t)||s.database.toLowerCase().includes(t)||s.schema.toLowerCase().includes(t)||s.tags?.toLowerCase().includes(t)||s.description?.toLowerCase().includes(t))}async getFilteredHistory(e,t,s,n){this.initialized||await this.initialize();let i=this.cache.filter(a=>!(e&&a.host!==e||t&&a.database!==t||s&&a.schema!==s));return n&&(i=i.slice(0,n)),i}async getArchivedHistory(){return[]}async clearArchive(){}close(){console.log("Query history manager closed")}}});var dt={};se(dt,{runQueriesSequentially:()=>qe,runQuery:()=>U,runQueryRaw:()=>ct});function st(l){let e=l.match(/SERVER=([^;]+)/i),t=l.match(/DATABASE=([^;]+)/i);return{host:e?e[1]:"unknown",database:t?t[1]:"unknown"}}function ot(l){let e=new Set;if(!l)return e;for(let t of l.matchAll(/\$\{([A-Za-z0-9_]+)\}/g))t[1]&&e.add(t[1]);return e}function it(l){if(!l)return{sql:"",setValues:{}};let e=l.split(/\r?\n/),t=[],s={};for(let n of e){let i=n.match(/^\s*@SET\s+([A-Za-z0-9_]+)\s*=\s*(.+)$/i);if(i){let a=i[2].trim();a.endsWith(";")&&(a=a.slice(0,-1).trim());let o=a.match(/^'(.*)'$/s)||a.match(/^"(.*)"$/s);o&&(a=o[1]),s[i[1]]=a}else t.push(n)}return{sql:t.join(`
`),setValues:s}}async function rt(l,e,t){let s={};if(l.size===0)return s;if(e){let i=Array.from(l).filter(a=>!(t&&t[a]!==void 0));if(i.length>0)throw new Error("Query contains variables but silent mode is enabled; cannot prompt for values. Missing: "+i.join(", "));for(let a of l)s[a]=t[a];return s}let n=[];for(let i of l)t&&t[i]!==void 0?s[i]=t[i]:n.push(i);for(let i of n){let a=await De.window.showInputBox({prompt:`Enter value for ${i}`,placeHolder:"",value:t&&t[i]?t[i]:void 0,ignoreFocusOut:!0});if(a===void 0)throw new Error("Variable input cancelled by user");s[i]=a}return s}function at(l,e){return l.replace(/\$\{([A-Za-z0-9_]+)\}/g,(t,s)=>e[s]??"")}async function ct(l,e,t=!1,s,n,i){if(!we)throw new Error("odbc package not installed. Please run: npm install odbc");let a=s||new ue(l),o=a.getKeepConnectionOpen(),r;t||(r=De.window.createOutputChannel("Netezza SQL"),r.show(!0),r.appendLine("Executing query..."),n&&r.appendLine(`Target Connection: ${n}`));try{let p=it(e),u=p.sql,h=p.setValues,g=ot(u);if(g.size>0){let D=await rt(g,t,h);u=at(u,D)}let m,b=!0,T,R=n;if(!R&&i&&(R=a.getConnectionForExecution(i)),R||(R=a.getActiveConnectionName()||void 0),o){m=await a.getPersistentConnection(R),b=!1;let D=await a.getConnectionString(R);if(!D)throw new Error("Connection not configured. Please connect via Netezza: Connect...");T=D}else{let D=await a.getConnectionString(R);if(!D)throw new Error("Connection not configured. Please connect via Netezza: Connect...");T=D,m=await we.connect({connectionString:T,fetchArray:!0})}try{let D=await lt(m,u,2e5),x="unknown";try{let O=await m.query("SELECT CURRENT_SCHEMA");O&&O.length>0&&(Array.isArray(O[0])?x=O[0][0]||"unknown":x=O[0].CURRENT_SCHEMA||"unknown")}catch(O){console.debug("Could not retrieve current schema:",O)}let M=st(T);if(new Q(l).addEntry(M.host,M.database,x,e,R).catch(O=>{console.error("Failed to log query to history:",O)}),D&&Array.isArray(D)){let O=D.columns?D.columns.map(W=>({name:W.name,type:W.dataType})):[];return r&&r.appendLine("Query completed."),{columns:O,data:D,rowsAffected:D.count,limitReached:D.limitReached,sql:u}}else return r&&r.appendLine("Query executed successfully (no results)."),{columns:[],data:[],rowsAffected:D?.count,message:"Query executed successfully (no results).",sql:u}}finally{b&&await m.close()}}catch(p){let u=We(p);throw r&&r.appendLine(u),new Error(u)}}async function U(l,e,t=!1,s,n,i){try{let a=await ct(l,e,t,n,s,i);if(a.data&&a.data.length>0){let o=a.data.map(p=>{let u={};return a.columns.forEach((h,g)=>{u[h.name]=p[g]}),u});return JSON.stringify(o,(p,u)=>typeof u=="bigint"?u>=Number.MIN_SAFE_INTEGER&&u<=Number.MAX_SAFE_INTEGER?Number(u):u.toString():u,2)}else if(a.message)return a.message;return}catch(a){throw a}}async function qe(l,e,t,s){if(!we)throw new Error("odbc package not installed. Please run: npm install odbc");let n=t||new ue(l),i=n.getKeepConnectionOpen(),a=De.window.createOutputChannel("Netezza SQL");a.show(!0),a.appendLine(`Executing ${e.length} queries sequentially...`);let o=[],r=t?t.getConnectionForExecution(s):void 0;r||(r=n.getActiveConnectionName()||void 0);try{let p,u=!0,h;if(i){p=await n.getPersistentConnection(r),u=!1;let g=await n.getConnectionString(r);if(!g)throw new Error("Connection not configured. Please connect via Netezza: Connect...");h=g}else{let g=await n.getConnectionString(r);if(!g)throw new Error("Connection not configured. Please connect via Netezza: Connect...");h=g,p=await we.connect({connectionString:h,fetchArray:!0})}try{let g="unknown";try{let R=await p.query("SELECT CURRENT_SCHEMA");R&&R.length>0&&(Array.isArray(R[0])?g=R[0][0]||"unknown":g=R[0].CURRENT_SCHEMA||"unknown")}catch(R){console.debug("Could not retrieve current schema:",R)}let m=st(h),b=new Q(l),T=r;for(let R=0;R<e.length;R++){let D=e[R];a.appendLine(`Executing query ${R+1}/${e.length}...`);try{let x=it(D),M=x.sql,L=x.setValues,O=ot(M);if(O.size>0){let Y=await rt(O,!1,L);M=at(M,Y)}let W=await lt(p,M,2e5);if(b.addEntry(m.host,m.database,g,D,T).catch(Y=>{console.error("Failed to log query to history:",Y)}),W&&Array.isArray(W)){let Y=W.columns?W.columns.map(G=>({name:G.name,type:G.dataType})):[];o.push({columns:Y,data:W,rowsAffected:W.count,limitReached:W.limitReached,sql:M})}else o.push({columns:[],data:[],rowsAffected:W?.count,message:"Query executed successfully",sql:M})}catch(x){let M=We(x);throw a.appendLine(`Error in query ${R+1}: ${M}`),new Error(M)}}a.appendLine("All queries completed.")}finally{u&&await p.close()}}catch(p){let u=We(p);throw a.appendLine(u),new Error(u)}return o}function We(l){return l.odbcErrors&&Array.isArray(l.odbcErrors)&&l.odbcErrors.length>0?l.odbcErrors.map(e=>`[ODBC Error] State: ${e.state}, Native Code: ${e.code}
Message: ${e.message}`).join(`

`):`Error: ${l.message||l}`}async function lt(l,e,t){let s=await l.createStatement();try{await s.prepare(e);let n=await s.execute({cursor:!0,fetchSize:5e3}),i=[],a=0,o=await n.fetch(),r=o?o.columns:void 0,p=o.count;for(;o&&o.length>0;){!r&&o.columns&&(r=o.columns);for(let u of o)if(i.push(u),a++,a>=t){i.limitReached=!0;break}if(a>=t){i.limitReached=!0;break}o=await n.fetch()}return await n.close(),r&&(i.columns=r),i.count=p,i}catch(n){throw n}finally{await s.close()}}var De,we,me=ne(()=>{"use strict";De=B(require("vscode"));Xe();Ie();try{we=require("odbc")}catch{console.error("odbc package not installed. Run: npm install odbc")}});var ut={};se(ut,{SchemaItem:()=>ee,SchemaProvider:()=>Ee});var _,Ee,ee,Ve=ne(()=>{"use strict";_=B(require("vscode"));me();Ee=class{constructor(e,t,s){this.context=e;this.connectionManager=t;this.metadataCache=s;this._onDidChangeTreeData=new _.EventEmitter;this.onDidChangeTreeData=this._onDidChangeTreeData.event;this.connectionManager.onDidChangeConnections(()=>this.refresh())}refresh(){this._onDidChangeTreeData.fire()}getTreeItem(e){return e}getParent(e){if(e.contextValue!=="serverInstance"){if(e.contextValue==="database")return new ee(e.connectionName,_.TreeItemCollapsibleState.Collapsed,"serverInstance",void 0,void 0,void 0,void 0,void 0,e.connectionName);if(e.contextValue==="typeGroup")return new ee(e.dbName,_.TreeItemCollapsibleState.Collapsed,"database",e.dbName,void 0,void 0,void 0,void 0,e.connectionName);if(e.contextValue.startsWith("netezza:"))return new ee(e.objType,_.TreeItemCollapsibleState.Collapsed,"typeGroup",e.dbName,e.objType,void 0,void 0,void 0,e.connectionName)}}async getChildren(e){if(e){if(e.contextValue==="serverInstance")try{if(!e.connectionName)return[];let t=await U(this.context,"SELECT DATABASE FROM system.._v_database ORDER BY DATABASE",!0,e.connectionName,this.connectionManager);return t?JSON.parse(t).map(n=>new ee(n.DATABASE,_.TreeItemCollapsibleState.Collapsed,"database",n.DATABASE,void 0,void 0,void 0,void 0,e.connectionName)):[]}catch(t){return _.window.showErrorMessage(`Failed to load databases for ${e.connectionName}: ${t}`),[]}else if(e.contextValue==="database")try{let t=`SELECT DISTINCT OBJTYPE FROM ${e.dbName}.._V_OBJECT_DATA WHERE DBNAME = '${e.dbName}' ORDER BY OBJTYPE`,s=await U(this.context,t,!0,e.connectionName,this.connectionManager);return JSON.parse(s||"[]").map(i=>new ee(i.OBJTYPE,_.TreeItemCollapsibleState.Collapsed,"typeGroup",e.dbName,i.OBJTYPE,void 0,void 0,void 0,e.connectionName))}catch(t){return _.window.showErrorMessage("Failed to load object types: "+t),[]}else if(e.contextValue==="typeGroup")try{let t=`SELECT OBJNAME, SCHEMA, OBJID, COALESCE(DESCRIPTION, '') AS DESCRIPTION FROM ${e.dbName}.._V_OBJECT_DATA WHERE DBNAME = '${e.dbName}' AND OBJTYPE = '${e.objType}' ORDER BY OBJNAME`,s=await U(this.context,t,!0,e.connectionName,this.connectionManager);return JSON.parse(s||"[]").map(i=>{let o=["TABLE","VIEW","EXTERNAL TABLE","SYSTEM VIEW","SYSTEM TABLE"].includes(e.objType||"");return new ee(i.OBJNAME,o?_.TreeItemCollapsibleState.Collapsed:_.TreeItemCollapsibleState.None,`netezza:${e.objType}`,e.dbName,e.objType,i.SCHEMA,i.OBJID,i.DESCRIPTION,e.connectionName)})}catch(t){return _.window.showErrorMessage("Failed to load objects: "+t),[]}else if(e.contextValue.startsWith("netezza:")&&e.objId)try{let t=`SELECT 
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
                        X.ATTNUM`,s=await U(this.context,t,!0,e.connectionName,this.connectionManager);return JSON.parse(s||"[]").map(i=>new ee(`${i.ATTNAME} (${i.FORMAT_TYPE})`,_.TreeItemCollapsibleState.None,"column",e.dbName,void 0,void 0,void 0,i.DESCRIPTION,e.connectionName))}catch(t){return _.window.showErrorMessage("Failed to load columns: "+t),[]}}else return(await this.connectionManager.getConnections()).map(s=>new ee(s.name,_.TreeItemCollapsibleState.Collapsed,"serverInstance",void 0,void 0,void 0,void 0,void 0,s.name));return[]}},ee=class extends _.TreeItem{constructor(t,s,n,i,a,o,r,p,u){super(t,s);this.label=t;this.collapsibleState=s;this.contextValue=n;this.dbName=i;this.objType=a;this.schema=o;this.objId=r;this.objectDescription=p;this.connectionName=u;let h=this.label;u&&(h+=`
[Server: ${u}]`),p&&p.trim()&&(h+=`

${p.trim()}`),o&&n.startsWith("netezza:")&&(h+=`

Schema: ${o}`),this.tooltip=h,this.description=o?`(${o})`:"",n==="serverInstance"?this.iconPath=new _.ThemeIcon("server"):n==="database"?this.iconPath=new _.ThemeIcon("database"):n==="typeGroup"?this.iconPath=new _.ThemeIcon("folder"):n.startsWith("netezza:")?this.iconPath=this.getIconForType(a):n==="column"&&(this.iconPath=new _.ThemeIcon("symbol-field"))}getIconForType(t){switch(t){case"TABLE":return new _.ThemeIcon("table");case"VIEW":return new _.ThemeIcon("eye");case"PROCEDURE":return new _.ThemeIcon("gear");case"FUNCTION":return new _.ThemeIcon("symbol-function");case"AGGREGATE":return new _.ThemeIcon("symbol-operator");case"EXTERNAL TABLE":return new _.ThemeIcon("server");default:return new _.ThemeIcon("file")}}}});var ht={};se(ht,{generateDDL:()=>Wt});function F(l){return!l||/^[A-Z_][A-Z0-9_]*$/i.test(l)&&l===l.toUpperCase()?l:`"${l.replace(/"/g,'""')}"`}async function mt(l,e,t,s){let n=`
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
    `,i=await l.query(n),a=[];for(let o of i)a.push({name:o.ATTNAME,description:o.DESCRIPTION||null,fullTypeName:o.FULL_TYPE,notNull:!!o.ATTNOTNULL,defaultValue:o.COLDEFAULT||null});return a}async function Pt(l,e,t,s){try{let n=`
            SELECT ATTNAME
            FROM ${e.toUpperCase()}.._V_TABLE_DIST_MAP
            WHERE SCHEMA = '${t.toUpperCase()}'
                AND TABLENAME = '${s.toUpperCase()}'
            ORDER BY DISTSEQNO
        `;return(await l.query(n)).map(a=>a.ATTNAME)}catch{return[]}}async function _t(l,e,t,s){try{let n=`
            SELECT ATTNAME
            FROM ${e.toUpperCase()}.._V_TABLE_ORGANIZE_COLUMN
            WHERE SCHEMA = '${t.toUpperCase()}'
                AND TABLENAME = '${s.toUpperCase()}'
            ORDER BY ORGSEQNO
        `;return(await l.query(n)).map(a=>a.ATTNAME)}catch{return[]}}async function Bt(l,e,t,s){let n=`
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
    `,i=new Map;try{let a=await l.query(n);for(let o of a){let r=o.CONSTRAINTNAME;if(!i.has(r)){let u={p:"PRIMARY KEY",f:"FOREIGN KEY",u:"UNIQUE"};i.set(r,{type:u[o.CONTYPE]||"UNKNOWN",typeChar:o.CONTYPE,columns:[],pkDatabase:o.PKDATABASE||null,pkSchema:o.PKSCHEMA||null,pkRelation:o.PKRELATION||null,pkColumns:[],updateType:o.UPDT_TYPE||"NO ACTION",deleteType:o.DEL_TYPE||"NO ACTION"})}let p=i.get(r);p.columns.push(o.ATTNAME),o.PKATTNAME&&p.pkColumns.push(o.PKATTNAME)}}catch(a){console.warn("Cannot retrieve keys info:",a)}return i}async function Ut(l,e,t,s){try{let n=`
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
            `,i=await l.query(n);if(i.length>0&&i[0].DESCRIPTION)return i[0].DESCRIPTION}catch{}}return null}async function zt(l,e,t,s){let n=await mt(l,e,t,s);if(n.length===0)throw new Error(`Table ${e}.${t}.${s} not found or has no columns`);let i=await Pt(l,e,t,s),a=await _t(l,e,t,s),o=await Bt(l,e,t,s),r=await Ut(l,e,t,s),p=F(e),u=F(t),h=F(s),g=[];g.push(`CREATE TABLE ${p}.${u}.${h}`),g.push("(");let m=[];for(let b of n){let R=`    ${F(b.name)} ${b.fullTypeName}`;b.defaultValue&&(R+=` DEFAULT ${b.defaultValue}`),m.push(R)}if(g.push(m.join(`,
`)),i.length>0){let b=i.map(T=>F(T));g.push(`)
DISTRIBUTE ON (${b.join(", ")})`)}else g.push(`)
DISTRIBUTE ON RANDOM`);if(a.length>0){let b=a.map(T=>F(T));g.push(`ORGANIZE ON (${b.join(", ")})`)}g.push(";"),g.push("");for(let[b,T]of o){let R=F(b),D=T.columns.map(x=>F(x));if(T.typeChar==="f"){let x=T.pkColumns.filter(M=>M).map(M=>F(M));x.length>0&&g.push(`ALTER TABLE ${p}.${u}.${h} ADD CONSTRAINT ${R} ${T.type} (${D.join(", ")}) REFERENCES ${T.pkDatabase}.${T.pkSchema}.${T.pkRelation} (${x.join(", ")}) ON DELETE ${T.deleteType} ON UPDATE ${T.updateType};`)}else(T.typeChar==="p"||T.typeChar==="u")&&g.push(`ALTER TABLE ${p}.${u}.${h} ADD CONSTRAINT ${R} ${T.type} (${D.join(", ")});`)}if(r){let b=r.replace(/'/g,"''");g.push(""),g.push(`COMMENT ON TABLE ${p}.${u}.${h} IS '${b}';`)}for(let b of n)if(b.description){let T=F(b.name),R=b.description.replace(/'/g,"''");g.push(`COMMENT ON COLUMN ${p}.${u}.${h}.${T} IS '${R}';`)}return g.join(`
`)}async function Ft(l,e,t,s){let n=`
        SELECT 
            SCHEMA,
            VIEWNAME,
            DEFINITION,
            OBJID::INT
        FROM ${e.toUpperCase()}.._V_VIEW
        WHERE DATABASE = '${e.toUpperCase()}'
            AND SCHEMA = '${t.toUpperCase()}'
            AND VIEWNAME = '${s.toUpperCase()}'
    `,a=await l.query(n);if(a.length===0)throw new Error(`View ${e}.${t}.${s} not found`);let o=a[0],r=F(e),p=F(t),u=F(s),h=[];return h.push(`CREATE OR REPLACE VIEW ${r}.${p}.${u} AS`),h.push(o.DEFINITION||""),h.join(`
`)}async function kt(l,e,t,s){let n=`
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
    `,a=await l.query(n);if(a.length===0)throw new Error(`Procedure ${e}.${t}.${s} not found`);let o=a[0],r={schema:o.SCHEMA,procedureSource:o.PROCEDURESOURCE,objId:o.OBJID,returns:o.RETURNS,executeAsOwner:!!o.EXECUTEDASOWNER,description:o.DESCRIPTION||null,procedureSignature:o.PROCEDURESIGNATURE,arguments:o.ARGUMENTS||null},p=F(e),u=F(t),h=F(s),g=[];if(g.push(`CREATE OR REPLACE PROCEDURE ${p}.${u}.${h}`),g.push(`RETURNS ${r.returns}`),r.executeAsOwner?g.push("EXECUTE AS OWNER"):g.push("EXECUTE AS CALLER"),g.push("LANGUAGE NZPLSQL AS"),g.push("BEGIN_PROC"),g.push(r.procedureSource),g.push("END_PROC;"),r.description){let m=r.description.replace(/'/g,"''");g.push(`COMMENT ON PROCEDURE ${h} IS '${m}';`)}return g.join(`
`)}async function Ht(l,e,t,s){let n=`
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
    `,a=await l.query(n);if(a.length===0)throw new Error(`External table ${e}.${t}.${s} not found`);let o=a[0],r={schema:o.SCHEMA,tableName:o.TABLENAME,dataObject:o.EXTOBJNAME||null,delimiter:o.DELIM||null,encoding:o.ENCODING||null,timeStyle:o.TIMESTYLE||null,remoteSource:o.REMOTESOURCE||null,skipRows:o.SKIPROWS||null,maxErrors:o.MAXERRORS||null,escapeChar:o.ESCAPE||null,logDir:o.LOGDIR||null,decimalDelim:o.DECIMALDELIM||null,quotedValue:o.QUOTEDVALUE||null,nullValue:o.NULLVALUE||null,crInString:o.CRINSTRING??null,truncString:o.TRUNCSTRING??null,ctrlChars:o.CTRLCHARS??null,ignoreZero:o.IGNOREZERO??null,timeExtraZeros:o.TIMEEXTRAZEROS??null,y2Base:o.Y2BASE||null,fillRecord:o.FILLRECORD??null,compress:o.COMPRESS||null,includeHeader:o.INCLUDEHEADER??null,lfInString:o.LFINSTRING??null,dateStyle:o.DATESTYLE||null,dateDelim:o.DATEDELIM||null,timeDelim:o.TIMEDELIM||null,boolStyle:o.BOOLSTYLE||null,format:o.FORMAT||null,socketBufSize:o.SOCKETBUFSIZE||null,recordDelim:o.RECORDDELIM?String(o.RECORDDELIM).replace(/\r/g,"\\r").replace(/\n/g,"\\n"):null,maxRows:o.MAXROWS||null,requireQuotes:o.REQUIREQUOTES??null,recordLength:o.RECORDLENGTH||null,dateTimeDelim:o.DATETIMEDELIM||null,rejectFile:o.REJECTFILE||null},p=await mt(l,e,t,s),u=F(e),h=F(t),g=F(s),m=[];m.push(`CREATE EXTERNAL TABLE ${u}.${h}.${g}`),m.push("(");let b=p.map(T=>`    ${F(T.name)} ${T.fullTypeName}`);return m.push(b.join(`,
`)),m.push(")"),m.push("USING"),m.push("("),r.dataObject!==null&&m.push(`    DATAOBJECT('${r.dataObject}')`),r.delimiter!==null&&m.push(`    DELIMITER '${r.delimiter}'`),r.encoding!==null&&m.push(`    ENCODING '${r.encoding}'`),r.timeStyle!==null&&m.push(`    TIMESTYLE '${r.timeStyle}'`),r.remoteSource!==null&&m.push(`    REMOTESOURCE '${r.remoteSource}'`),r.maxErrors!==null&&m.push(`    MAXERRORS ${r.maxErrors}`),r.escapeChar!==null&&m.push(`    ESCAPECHAR '${r.escapeChar}'`),r.decimalDelim!==null&&m.push(`    DECIMALDELIM '${r.decimalDelim}'`),r.logDir!==null&&m.push(`    LOGDIR '${r.logDir}'`),r.quotedValue!==null&&m.push(`    QUOTEDVALUE '${r.quotedValue}'`),r.nullValue!==null&&m.push(`    NULLVALUE '${r.nullValue}'`),r.crInString!==null&&m.push(`    CRINSTRING ${r.crInString}`),r.truncString!==null&&m.push(`    TRUNCSTRING ${r.truncString}`),r.ctrlChars!==null&&m.push(`    CTRLCHARS ${r.ctrlChars}`),r.ignoreZero!==null&&m.push(`    IGNOREZERO ${r.ignoreZero}`),r.timeExtraZeros!==null&&m.push(`    TIMEEXTRAZEROS ${r.timeExtraZeros}`),r.y2Base!==null&&m.push(`    Y2BASE ${r.y2Base}`),r.fillRecord!==null&&m.push(`    FILLRECORD ${r.fillRecord}`),r.compress!==null&&m.push(`    COMPRESS ${r.compress}`),r.includeHeader!==null&&m.push(`    INCLUDEHEADER ${r.includeHeader}`),r.lfInString!==null&&m.push(`    LFINSTRING ${r.lfInString}`),r.dateStyle!==null&&m.push(`    DATESTYLE '${r.dateStyle}'`),r.dateDelim!==null&&m.push(`    DATEDELIM '${r.dateDelim}'`),r.timeDelim!==null&&m.push(`    TIMEDELIM '${r.timeDelim}'`),r.boolStyle!==null&&m.push(`    BOOLSTYLE '${r.boolStyle}'`),r.format!==null&&m.push(`    FORMAT '${r.format}'`),r.socketBufSize!==null&&m.push(`    SOCKETBUFSIZE ${r.socketBufSize}`),r.recordDelim!==null&&m.push(`    RECORDDELIM '${r.recordDelim}'`),r.maxRows!==null&&m.push(`    MAXROWS ${r.maxRows}`),r.requireQuotes!==null&&m.push(`    REQUIREQUOTES ${r.requireQuotes}`),r.recordLength!==null&&m.push(`    RECORDLENGTH ${r.recordLength}`),r.dateTimeDelim!==null&&m.push(`    DATETIMEDELIM '${r.dateTimeDelim}'`),r.rejectFile!==null&&m.push(`    REJECTFILE '${r.rejectFile}'`),m.push(");"),m.join(`
`)}async function Xt(l,e,t,s){let n=`
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
    `,a=await l.query(n);if(a.length===0)throw new Error(`Synonym ${e}.${t}.${s} not found`);let o=a[0],r=F(e),p=F(o.OWNER||t),u=F(s),h=o.REFOBJNAME,g=[];if(g.push(`CREATE SYNONYM ${r}.${p}.${u} FOR ${h};`),o.DESCRIPTION){let m=o.DESCRIPTION.replace(/'/g,"''");g.push(`COMMENT ON SYNONYM ${u} IS '${m}';`)}return g.join(`
`)}async function Wt(l,e,t,s,n){let i=null;try{i=await pt.connect(l);let a=n.toUpperCase();return a==="TABLE"?{success:!0,ddlCode:await zt(i,e,t,s),objectInfo:{database:e,schema:t,objectName:s,objectType:n}}:a==="VIEW"?{success:!0,ddlCode:await Ft(i,e,t,s),objectInfo:{database:e,schema:t,objectName:s,objectType:n}}:a==="PROCEDURE"?{success:!0,ddlCode:await kt(i,e,t,s),objectInfo:{database:e,schema:t,objectName:s,objectType:n}}:a==="EXTERNAL TABLE"?{success:!0,ddlCode:await Ht(i,e,t,s),objectInfo:{database:e,schema:t,objectName:s,objectType:n}}:a==="SYNONYM"?{success:!0,ddlCode:await Xt(i,e,t,s),objectInfo:{database:e,schema:t,objectName:s,objectType:n}}:{success:!0,ddlCode:`-- DDL generation for ${n} not yet implemented
-- Object: ${e}.${t}.${s}
-- Type: ${n}
--
-- This feature can be extended to support:
-- - FUNCTION: Query _V_FUNCTION system table
-- - AGGREGATE: Query _V_AGGREGATE system table
`,objectInfo:{database:e,schema:t,objectName:s,objectType:n},note:`${n} DDL generation not yet implemented`}}catch(a){return{success:!1,error:`DDL generation error: ${a.message||a}`}}finally{if(i)try{await i.close()}catch{}}}var pt,gt=ne(()=>{"use strict";pt=B(require("odbc"))});var be={};se(be,{copyFileToClipboard:()=>Ye,exportCsvToXlsb:()=>Jt,exportQueryToXlsb:()=>Vt,getTempFilePath:()=>Yt});async function Vt(l,e,t,s=!1,n){let i=null;try{n&&n("Connecting to database..."),i=await qt.connect(l),n&&n("Executing query...");let a=await i.query(e);if(!a||!a.columns||a.columns.length===0)return{success:!1,message:"Query did not return any results (no columns)"};let o=a.columns.length;n&&n(`Query returned ${o} columns`);let r=a.columns.map(M=>M.name),p=a.map(M=>r.map(L=>M[L])),u=p.length;n&&n(`Writing ${u.toLocaleString()} rows to XLSX: ${t}`);let h=V.utils.book_new(),g=[r,...p],m=V.utils.aoa_to_sheet(g);V.utils.book_append_sheet(h,m,"Query Results");let b=[["SQL Query:"],...e.split(`
`).map(M=>[M])],T=V.utils.aoa_to_sheet(b);V.utils.book_append_sheet(h,T,"SQL Code"),V.writeFile(h,t,{bookType:"xlsx",compression:!0});let D=Je.statSync(t).size/(1024*1024);n&&(n("XLSX file created successfully"),n(`  - Rows: ${u.toLocaleString()}`),n(`  - Columns: ${o}`),n(`  - File size: ${D.toFixed(1)} MB`),n(`  - Location: ${t}`));let x={success:!0,message:`Successfully exported ${u} rows to ${t}`,details:{rows_exported:u,columns:o,file_size_mb:parseFloat(D.toFixed(1)),file_path:t}};if(s){n&&n("Copying to clipboard...");let M=await Ye(t);x.details&&(x.details.clipboard_success=M)}return x}catch(a){return{success:!1,message:`Export error: ${a.message||a}`}}finally{if(i)try{await i.close()}catch{}}}async function Jt(l,e,t=!1,s,n){try{n&&n("Reading CSV content...");let i=V.read(l,{type:"string",raw:!0});if(!i.SheetNames||i.SheetNames.length===0)return{success:!1,message:"CSV content is empty or invalid"};let a=i.Sheets[i.SheetNames[0]],o=V.utils.sheet_to_json(a,{header:1});if(o.length===0)return{success:!1,message:"CSV file is empty or contains no headers"};let r=o.length-1,p=o[0]?o[0].length:0;n&&n(`Writing ${r.toLocaleString()} rows to XLSX: ${e}`);let u=V.utils.book_new(),h=V.utils.aoa_to_sheet(o);if(V.utils.book_append_sheet(u,h,"CSV Data"),s?.sql){let T=[["SQL Query:"],...s.sql.split(`
`).map(D=>[D])],R=V.utils.aoa_to_sheet(T);V.utils.book_append_sheet(u,R,"SQL")}else{let T=s?.source||"Clipboard",R=[["CSV Source:"],[T]],D=V.utils.aoa_to_sheet(R);V.utils.book_append_sheet(u,D,"CSV Source")}V.writeFile(u,e,{bookType:"xlsx",compression:!0});let m=Je.statSync(e).size/(1024*1024);n&&(n("XLSX file created successfully"),n(`  - Rows: ${r.toLocaleString()}`),n(`  - Columns: ${p}`),n(`  - File size: ${m.toFixed(1)} MB`),n(`  - Location: ${e}`));let b={success:!0,message:`Successfully exported ${r} rows from CSV to ${e}`,details:{rows_exported:r,columns:p,file_size_mb:parseFloat(m.toFixed(1)),file_path:e}};if(t){n&&n("Copying to clipboard...");let T=await Ye(e);b.details&&(b.details.clipboard_success=T)}return b}catch(i){return{success:!1,message:`Export error: ${i.message||i}`}}}async function Ye(l){return Be.platform()!=="win32"?(console.error("Clipboard file copy is only supported on Windows"),!1):new Promise(e=>{try{let t=ge.normalize(ge.resolve(l)),s=`Set-Clipboard -Path "${t.replace(/"/g,'`"')}"`,n=(0,ft.spawn)("powershell.exe",["-NoProfile","-NonInteractive","-Command",s]),i="";n.stderr.on("data",a=>{i+=a.toString()}),n.on("close",a=>{a!==0?(console.error(`PowerShell clipboard copy failed: ${i}`),e(!1)):(console.log(`File copied to clipboard: ${t}`),e(!0))}),n.on("error",a=>{console.error(`Error spawning PowerShell: ${a.message}`),e(!1)})}catch(t){console.error(`Error copying file to clipboard: ${t.message}`),e(!1)}})}function Yt(){let l=Be.tmpdir(),t=`netezza_export_${Date.now()}.xlsx`;return ge.join(l,t)}var V,Je,ge,Be,ft,qt,Te=ne(()=>{"use strict";V=B(require("xlsx")),Je=B(require("fs")),ge=B(require("path")),Be=B(require("os")),ft=require("child_process"),qt=require("odbc")});var Et={};se(Et,{exportToCsv:()=>jt});async function jt(l,e,t,s,n){if(!Qe)throw new Error("odbc package not installed. Please run: npm install odbc");let i=await Qe.connect(e);try{n&&n.report({message:"Executing query..."});let a=await i.query(t,{cursor:!0,fetchSize:1e3});n&&n.report({message:"Writing to CSV..."});let o=wt.createWriteStream(s,{encoding:"utf8",highWaterMark:64*1024}),r=[];a.columns&&(r=a.columns.map(m=>m.name)),r.length>0&&o.write(r.map(je).join(",")+`
`);let p=0,u=[],h=[],g=100;do{u=await a.fetch();for(let m of u){p++;let b;if(r.length>0?b=r.map(T=>je(m[T])):b=Object.values(m).map(T=>je(T)),h.push(b.join(",")),h.length>=g){let T=o.write(h.join(`
`)+`
`);h=[],T||await new Promise(R=>o.once("drain",R))}}n&&u.length>0&&n.report({message:`Processed ${p} rows...`})}while(u.length>0&&!a.noData);h.length>0&&o.write(h.join(`
`)+`
`),await a.close(),o.end(),await new Promise((m,b)=>{o.on("finish",m),o.on("error",b)}),n&&n.report({message:`Completed: ${p} rows exported`})}finally{try{await i.close()}catch(a){console.error("Error closing connection:",a)}}}function je(l){if(l==null)return"";let e="";return typeof l=="bigint"?l>=Number.MIN_SAFE_INTEGER&&l<=Number.MAX_SAFE_INTEGER?e=Number(l).toString():e=l.toString():l instanceof Date?e=l.toISOString():l instanceof Buffer?e=l.toString("hex"):typeof l=="object"?e=JSON.stringify(l):e=String(l),e.includes('"')||e.includes(",")||e.includes(`
`)||e.includes("\r")?`"${e.replace(/"/g,'""')}"`:e}var wt,Qe,vt=ne(()=>{"use strict";wt=B(require("fs"));try{Qe=require("odbc")}catch{console.error("odbc package not installed. Run: npm install odbc")}});var yt={};se(yt,{ColumnTypeChooser:()=>fe,NetezzaDataType:()=>oe,NetezzaImporter:()=>Ue,importDataToNetezza:()=>Qt});async function Qt(l,e,t,s,n){let i=Date.now();try{if(!l||!j.existsSync(l))return{success:!1,message:`Source file not found: ${l}`};if(!e)return{success:!1,message:"Target table name is required"};if(!t)return{success:!1,message:"Connection string is required"};let o=j.statSync(l).size,r=ae.extname(l).toLowerCase(),p=[".csv",".txt",".xlsx",".xlsb"];if(!p.includes(r))return{success:!1,message:`Unsupported file format: ${r}. Supported: ${p.join(", ")}`};if([".xlsx",".xlsb"].includes(r)&&!Se)return{success:!1,message:"XLSX module not available. Please run: npm install xlsx"};n?.("Starting import process..."),n?.(`  Source file: ${l}`),n?.(`  Target table: ${e}`),n?.(`  File size: ${o.toLocaleString()} bytes`),n?.(`  File format: ${r}`);let h=new Ue(l,e,t);await h.analyzeDataTypes(n),n?.("Using file-based import...");let g=await h.createDataFile(n),m=h.generateCreateTableSql();if(n?.("Generated SQL:"),n?.(m),n?.("Connecting to Netezza..."),!Ge)throw new Error("ODBC module not available");let b=await Ge.connect(t);try{n?.("Executing CREATE TABLE with EXTERNAL data..."),await b.query(m),n?.("Import completed successfully")}finally{await b.close();try{j.existsSync(g)&&(j.unlinkSync(g),n?.("Temporary data file cleaned up"))}catch(R){n?.(`Warning: Could not clean up temp file: ${R.message}`)}}let T=(Date.now()-i)/1e3;return{success:!0,message:"Import completed successfully",details:{sourceFile:l,targetTable:e,fileSize:o,format:r,rowsProcessed:h.getRowsCount(),rowsInserted:h.getRowsCount(),processingTime:`${T.toFixed(1)}s`,columns:h.getSqlHeaders().length,detectedDelimiter:h.getCsvDelimiter()}}}catch(a){let o=(Date.now()-i)/1e3;return{success:!1,message:`Import failed: ${a.message}`,details:{processingTime:`${o.toFixed(1)}s`}}}}var j,ae,Se,Ge,oe,fe,Ue,Ke=ne(()=>{"use strict";j=B(require("fs")),ae=B(require("path"));try{Se=require("xlsx")}catch{console.error("XLSX module not available")}try{Ge=require("odbc")}catch{console.error("ODBC module not available")}oe=class{constructor(e,t,s,n){this.dbType=e;this.precision=t;this.scale=s;this.length=n}toString(){return["BIGINT","DATE","DATETIME"].includes(this.dbType)?this.dbType:this.dbType==="NUMERIC"?`${this.dbType}(${this.precision},${this.scale})`:this.dbType==="NVARCHAR"?`${this.dbType}(${this.length})`:`TODO !!! ${this.dbType}`}},fe=class{constructor(){this.decimalDelimInCsv=".";this.firstTime=!0;this.currentType=new oe("BIGINT")}getType(e){let t=this.currentType.dbType,s=e.length;if(t==="BIGINT"&&/^\d+$/.test(e)&&s<15)return this.firstTime=!1,new oe("BIGINT");let n=(e.match(new RegExp(`\\${this.decimalDelimInCsv}`,"g"))||[]).length;if(["BIGINT","NUMERIC"].includes(t)&&n<=1){let a=e.replace(this.decimalDelimInCsv,"");if(/^\d+$/.test(a)&&s<15&&(!a.startsWith("0")||n>0))return this.firstTime=!1,new oe("NUMERIC",16,6)}if((t==="DATE"||this.firstTime)&&(e.match(/-/g)||[]).length===2&&s>=8&&s<=10){let a=e.split("-");if(a.length===3&&a.every(o=>/^\d+$/.test(o)))try{let o=new Date(parseInt(a[0]),parseInt(a[1])-1,parseInt(a[2]));if(!isNaN(o.getTime()))return this.firstTime=!1,new oe("DATE")}catch{}}if((t==="DATETIME"||this.firstTime)&&(e.match(/-/g)||[]).length===2&&s>=12&&s<=20){let a=e.match(/^(\d{4})-(\d{1,2})-(\d{1,2})[\s|T](\d{2}):(\d{2})(:?(\d{2}))?$/);if(a)try{let o=a[7]?parseInt(a[7]):0,r=new Date(parseInt(a[1]),parseInt(a[2])-1,parseInt(a[3]),parseInt(a[4]),parseInt(a[5]),o);if(!isNaN(r.getTime()))return this.firstTime=!1,new oe("DATETIME")}catch{}}let i=Math.max(s+5,20);return this.currentType.length!==void 0&&i<this.currentType.length&&(i=this.currentType.length),this.firstTime=!1,new oe("NVARCHAR",void 0,void 0,i)}refreshCurrentType(e){return this.currentType=this.getType(e),this.currentType}},Ue=class{constructor(e,t,s,n){this.delimiter="	";this.delimiterPlain="\\t";this.recordDelim=`
`;this.recordDelimPlain="\\n";this.escapechar="\\";this.csvDelimiter=",";this.excelData=[];this.isExcelFile=!1;this.sqlHeaders=[];this.dataTypes=[];this.rowsCount=0;this.valuesToEscape=[];this.filePath=e,this.targetTable=t,this.connectionString=s,this.logDir=n||ae.join(ae.dirname(e),"netezza_logs");let i=ae.extname(e).toLowerCase();this.isExcelFile=[".xlsx",".xlsb"].includes(i);let a=Math.floor(Math.random()*1e3);this.pipeName=`\\\\.\\pipe\\NETEZZA_IMPORT_${a}`,this.valuesToEscape=[this.escapechar,this.recordDelim,"\r",this.delimiter],j.existsSync(this.logDir)||j.mkdirSync(this.logDir,{recursive:!0})}detectCsvDelimiter(){let t=j.readFileSync(this.filePath,"utf-8").split(`
`)[0]||"";t.startsWith("\uFEFF")&&(t=t.slice(1));let s=[";","	","|",","],n={};for(let a of s)n[a]=(t.match(new RegExp(a==="|"?"\\|":a,"g"))||[]).length;let i=Math.max(...Object.values(n));i>0&&(this.csvDelimiter=Object.keys(n).find(a=>n[a]===i)||",")}cleanColumnName(e){let t=String(e).trim();return t=t.replace(/[^0-9a-zA-Z]+/g,"_").toUpperCase(),(!t||/^\d/.test(t))&&(t="COL_"+t),t}parseCsvLine(e){let t=[],s="",n=!1;for(let i=0;i<e.length;i++){let a=e[i];a==='"'?n&&e[i+1]==='"'?(s+='"',i++):n=!n:a===this.csvDelimiter&&!n?(t.push(s),s=""):s+=a}return t.push(s),t}readExcelFile(e){if(!Se)throw new Error("XLSX module not available");e?.("Reading Excel file...");let t=Se.readFile(this.filePath,{type:"file"}),s=t.SheetNames[0];if(!s)throw new Error("Excel file has no sheets");e?.(`Processing sheet: ${s}`);let n=t.Sheets[s],a=Se.utils.sheet_to_json(n,{header:1,raw:!1,defval:""}).map(o=>o.map(r=>r!=null?String(r):""));return e?.(`Excel file loaded: ${a.length} rows, ${a[0]?.length||0} columns`),a}async analyzeDataTypes(e){e?.("Analyzing data types...");let t;if(this.isExcelFile)this.excelData=this.readExcelFile(e),t=this.excelData;else{this.detectCsvDelimiter();let n=j.readFileSync(this.filePath,"utf-8");n.startsWith("\uFEFF")&&(n=n.slice(1));let i=n.split(/\r?\n/);t=[];for(let a of i)a.trim()&&t.push(this.parseCsvLine(a))}if(!t||t.length===0)throw new Error("No data found in file");let s=[];this.sqlHeaders=t[0].map(n=>this.cleanColumnName(n||"COLUMN"));for(let n=0;n<t[0].length;n++)s.push(new fe);for(let n=1;n<t.length;n++){let i=t[n];for(let a=0;a<i.length;a++)a<s.length&&i[a]&&i[a].trim()&&s[a].refreshCurrentType(i[a].trim());n%1e4===0&&e?.(`Analyzed ${n.toLocaleString()} rows...`)}return this.rowsCount=t.length-1,e?.(`Analysis complete: ${this.rowsCount.toLocaleString()} rows`),this.dataTypes=s,s}escapeValue(e){let t=String(e).trim();for(let s of this.valuesToEscape)t=t.split(s).join(`${this.escapechar}${s}`);return t}formatValue(e,t){let s=this.escapeValue(e);return t<this.dataTypes.length&&this.dataTypes[t].currentType.dbType==="DATETIME"&&(s=s.replace("T"," ")),s}generateCreateTableSql(){let e=[];for(let s=0;s<this.sqlHeaders.length;s++){let n=this.sqlHeaders[s],i=this.dataTypes[s];e.push(`        ${n} ${i.currentType.toString()}`)}let t=this.logDir.replace(/\\/g,"/");return`CREATE TABLE ${this.targetTable} AS 
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
) DISTRIBUTE ON RANDOM;`}async createDataFile(e){let t=ae.join(this.logDir,`netezza_import_data_${Math.floor(Math.random()*1e3)}.txt`);e?.(`Creating temporary data file: ${t}`);try{let s;if(this.isExcelFile){if(!this.excelData||this.excelData.length===0)throw new Error("Excel data not loaded. Call analyzeDataTypes first.");s=this.excelData.slice(1)}else{let i=j.readFileSync(this.filePath,"utf-8");i.startsWith("\uFEFF")&&(i=i.slice(1));let a=i.split(/\r?\n/);s=[];let o=!0;for(let r of a)if(r.trim()){if(o){o=!1;continue}s.push(this.parseCsvLine(r))}}let n=[];for(let i=0;i<s.length;i++){let o=s[i].map((r,p)=>this.formatValue(r||"",p));n.push(o.join(this.delimiter)),(i+1)%1e4===0&&e?.(`Processed ${(i+1).toLocaleString()} rows...`)}return j.writeFileSync(t,n.join(this.recordDelim),"utf-8"),this.pipeName=t.replace(/\\/g,"/"),e?.(`Data file created: ${this.pipeName}`),t}catch(s){throw new Error(`Error creating data file: ${s.message}`)}}getRowsCount(){return this.rowsCount}getSqlHeaders(){return this.sqlHeaders}getCsvDelimiter(){return this.csvDelimiter}}});var bt={};se(bt,{ClipboardDataProcessor:()=>ze,importClipboardDataToNetezza:()=>en});function Gt(l){let e=String(l).trim();return e=e.replace(/[^0-9a-zA-Z]+/g,"_").toUpperCase(),(!e||/^\d/.test(e))&&(e="COL_"+e),e}function Kt(l,e,t){let s=String(l).trim();for(let n of t)s=s.split(n).join(`${e}${n}`);return s}function Zt(l,e,t,s,n){let i=Kt(l,s,n);return e<t.length&&t[e].currentType.dbType==="DATETIME"&&(i=i.replace("T"," ")),i}async function en(l,e,t,s,n){let i=Date.now(),a=null;try{if(!l)return{success:!1,message:"Target table name is required"};if(!e)return{success:!1,message:"Connection string is required"};n?.("Starting clipboard import process..."),n?.(`  Target table: ${l}`),n?.(`  Format preference: ${t||"auto-detect"}`);let o=new ze,[r,p]=await o.processClipboardData(t,n);if(!r||!r.length)return{success:!1,message:"No data found in clipboard"};if(r.length<2)return{success:!1,message:"Clipboard data must contain at least headers and one data row"};n?.(`  Detected format: ${p}`),n?.(`  Rows: ${r.length}`),n?.(`  Columns: ${r[0].length}`);let u=r[0].map(k=>Gt(k)),h=r.slice(1);n?.("Analyzing clipboard data types...");let g=u.map(()=>new fe);for(let k=0;k<h.length;k++){let le=h[k];for(let te=0;te<le.length;te++)te<g.length&&le[te]&&le[te].trim()&&g[te].refreshCurrentType(le[te].trim());(k+1)%1e3===0&&n?.(`Analyzed ${(k+1).toLocaleString()} rows...`)}n?.(`Analysis complete: ${h.length.toLocaleString()} data rows`);let m=Ze.join(require("os").tmpdir(),"netezza_clipboard_logs");ie.existsSync(m)||ie.mkdirSync(m,{recursive:!0});let b="	",T="\\t",R=`
`,D="\\n",x="\\",M=[x,R,"\r",b];a=Ze.join(m,`netezza_clipboard_import_${Math.floor(Math.random()*1e3)}.txt`),n?.(`Creating temporary data file: ${a}`);let L=[];for(let k=0;k<h.length;k++){let te=h[k].map((ke,He)=>Zt(ke,He,g,x,M));L.push(te.join(b)),(k+1)%1e3===0&&n?.(`Processed ${(k+1).toLocaleString()} rows...`)}ie.writeFileSync(a,L.join(R),"utf-8");let O=a.replace(/\\/g,"/");n?.(`Data file created: ${O}`);let W=[];for(let k=0;k<u.length;k++)W.push(`        ${u[k]} ${g[k].currentType.toString()}`);let Y=m.replace(/\\/g,"/"),G=`CREATE TABLE ${l} AS 
(
    SELECT * FROM EXTERNAL '${O}'
    (
${W.join(`,
`)}
    )
    USING
    (
        REMOTESOURCE 'odbc'
        DELIMITER '${T}'
        RecordDelim '${D}'
        ESCAPECHAR '${x}'
        NULLVALUE ''
        ENCODING 'utf-8'
        TIMESTYLE '24HOUR'
        SKIPROWS 0
        MAXERRORS 10
        LOGDIR '${Y}'
    )
) DISTRIBUTE ON RANDOM;`;if(n?.("Generated SQL:"),n?.(G),n?.("Connecting to Netezza..."),!et)throw new Error("ODBC module not available");let ce=await et.connect(e);try{n?.("Executing CREATE TABLE with EXTERNAL clipboard data..."),await ce.query(G),n?.("Clipboard import completed successfully")}finally{await ce.close()}let Fe=(Date.now()-i)/1e3;return{success:!0,message:"Clipboard import completed successfully",details:{targetTable:l,format:p,rowsProcessed:h.length,rowsInserted:h.length,processingTime:`${Fe.toFixed(1)}s`,columns:u.length,detectedDelimiter:b}}}catch(o){let r=(Date.now()-i)/1e3;return{success:!1,message:`Clipboard import failed: ${o.message}`,details:{processingTime:`${r.toFixed(1)}s`}}}finally{if(a&&ie.existsSync(a))try{ie.unlinkSync(a),n?.("Temporary clipboard data file cleaned up")}catch(o){n?.(`Warning: Could not clean up temp file: ${o.message}`)}}}var ie,Ze,Ct,et,ze,Tt=ne(()=>{"use strict";ie=B(require("fs")),Ze=B(require("path")),Ct=B(require("vscode"));Ke();try{et=require("odbc")}catch{console.error("ODBC module not available")}ze=class{constructor(){this.processedData=[]}processXmlSpreadsheet(e,t){t?.("Processing XML Spreadsheet data...");let s=[],n=0,i=[],a=0,o=e.match(/ExpandedColumnCount="(\d+)"/);o&&(n=parseInt(o[1]),t?.(`Table has ${n} columns`));let r=e.match(/ExpandedRowCount="(\d+)"/);r&&t?.(`Table has ${r[1]} rows`);let p=/<Row[^>]*>([\s\S]*?)<\/Row>/gi,u;for(;(u=p.exec(e))!==null;){let h=u[1];i=new Array(n).fill("");let g=/<Cell(?:\s+[^>]*ss:Index="(\d+)")?[^>]*>[\s\S]*?<Data[^>]*>([^<]*)<\/Data>[\s\S]*?<\/Cell>|<Cell(?:\s+[^>]*ss:Index="(\d+)")?[^>]*\/>/gi,m,b=0,T=/<Cell(?:[^>]*ss:Index="(\d+)")?[^>]*>(?:[\s\S]*?<Data[^>]*(?:\s+ss:Type="([^"]*)")?[^>]*>([^<]*)<\/Data>)?[\s\S]*?<\/Cell>/gi;for(;(m=T.exec(h))!==null;){m[1]&&(b=parseInt(m[1])-1);let R=m[2]||"",D=m[3]||"";R==="Boolean"&&(D=D==="0"?"False":"True"),b<n&&(i[b]=D),b++}i.some(R=>R.trim())&&s.push([...i]),a++,a%1e4===0&&t?.(`Analyzed ${a.toLocaleString()} rows...`)}return t?.(`XML processing complete: ${s.length} rows, ${n} columns`),this.processedData=s,s}processTextData(e,t){if(t?.("Processing text data..."),!e.trim())return[];let s=e.split(`
`);for(;s.length&&!s[s.length-1].trim();)s.pop();if(!s.length)return[];let n=["	",",",";","|"],i={};for(let p of n){let u=[];for(let h of s.slice(0,Math.min(5,s.length)))if(h.trim()){let g=h.split(p);u.push(g.length)}if(u.length){let h=u.reduce((m,b)=>m+b,0)/u.length,g=u.reduce((m,b)=>m+Math.pow(b-h,2),0)/u.length;i[p]=[h,-g]}}let a="	";Object.keys(i).length&&(a=Object.keys(i).reduce((p,u)=>{let[h,g]=i[p]||[0,0],[m,b]=i[u];return m>h||m===h&&b>g?u:p},"	")),t?.(`Auto-detected delimiter: '${a==="	"?"\\t":a}'`);let o=[],r=0;for(let p of s)if(p.trim()){let u=p.split(a).map(h=>h.trim());o.push(u),r=Math.max(r,u.length)}for(let p of o)for(;p.length<r;)p.push("");return t?.(`Text processing complete: ${o.length} rows, ${r} columns`),this.processedData=o,o}async getClipboardText(){return await Ct.env.clipboard.readText()}async processClipboardData(e,t){t?.("Getting clipboard data...");let s=await this.getClipboardText();if(!s)throw new Error("No data found in clipboard");t?.(`Data size: ${s.length} characters`);let n="TEXT";e==="XML Spreadsheet"||!e&&s.includes("<Workbook")&&s.includes("<Worksheet")?n="XML Spreadsheet":e==="TEXT"&&(n="TEXT"),t?.(`Detected format: ${n}`);let i;return n==="XML Spreadsheet"?i=this.processXmlSpreadsheet(s,t):i=this.processTextData(s,t),t?.(`Processed ${i.length} rows`),i.length&&t?.(`Columns per row: ${i[0].length}`),[i,n]}}});var sn={};se(sn,{activate:()=>tn,deactivate:()=>nn});module.exports=$t(sn);var c=B(require("vscode"));me();Xe();var Z=B(require("vscode")),Me=class l{constructor(e,t,s){this.extensionUri=t;this.connectionManager=s;this._disposables=[];this._panel=e,this._panel.onDidDispose(()=>this.dispose(),null,this._disposables),this._panel.webview.html=this._getHtmlForWebview(this._panel.webview),this._panel.webview.onDidReceiveMessage(async n=>{switch(n.command){case"save":try{await this.connectionManager.saveConnection(n.data),Z.window.showInformationMessage(`Connection '${n.data.name}' saved and activated!`),this.sendConnectionsToWebview()}catch(i){Z.window.showErrorMessage(`Error saving: ${i.message}`)}return;case"delete":try{await Z.window.showWarningMessage(`Are you sure you want to delete '${n.name}'?`,{modal:!0},"Yes","No")==="Yes"&&(await this.connectionManager.deleteConnection(n.name),Z.window.showInformationMessage(`Connection '${n.name}' deleted.`),this.sendConnectionsToWebview())}catch(i){Z.window.showErrorMessage(`Error deleting: ${i.message}`)}return;case"loadConnections":this.sendConnectionsToWebview();return}},null,this._disposables)}async sendConnectionsToWebview(){let e=this.connectionManager.getConnections(),t=this.connectionManager.getActiveConnectionName();await this._panel.webview.postMessage({command:"updateConnections",connections:e,activeName:t})}static createOrShow(e,t){let s=Z.window.activeTextEditor?Z.window.activeTextEditor.viewColumn:void 0;if(l.currentPanel){l.currentPanel._panel.reveal(s);return}let n=Z.window.createWebviewPanel("netezzaLogin","Connect to Netezza",s||Z.ViewColumn.One,{enableScripts:!0,retainContextWhenHidden:!0});l.currentPanel=new l(n,e,t)}dispose(){for(l.currentPanel=void 0,this._panel.dispose();this._disposables.length;){let e=this._disposables.pop();e&&e.dispose()}}_getHtmlForWebview(e){return`<!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Connect to Netezza</title>
            <style>
                body { font-family: var(--vscode-font-family); padding: 0; margin: 0; color: var(--vscode-editor-foreground); background-color: var(--vscode-editor-background); display: flex; height: 100vh; }
                .sidebar { width: 250px; border-right: 1px solid var(--vscode-panel-border); background-color: var(--vscode-sideBar-background); padding: 10px; overflow-y: auto; }
                .main { flex: 1; padding: 20px; overflow-y: auto; }
                
                .connection-item { padding: 8px; cursor: pointer; display: flex; align-items: center; border-radius: 3px; margin-bottom: 2px; }
                .connection-item:hover { background-color: var(--vscode-list-hoverBackground); }
                .connection-item.active { background-color: var(--vscode-list-activeSelectionBackground); color: var(--vscode-list-activeSelectionForeground); }
                .connection-item .name { flex: 1; font-weight: 500; }
                .connection-item .status { font-size: 0.8em; margin-left: 5px; opacity: 0.7; }
                
                .form-group { margin-bottom: 15px; }
                label { display: block; margin-bottom: 5px; font-weight: bold; }
                input { width: 100%; padding: 8px; box-sizing: border-box; background: var(--vscode-input-background); color: var(--vscode-input-foreground); border: 1px solid var(--vscode-input-border); }
                input:focus { border-color: var(--vscode-focusBorder); outline: none; }
                
                .actions { margin-top: 20px; display: flex; gap: 10px; }
                button { background: var(--vscode-button-background); color: var(--vscode-button-foreground); padding: 8px 16px; border: none; cursor: pointer; border-radius: 2px; }
                button:hover { background: var(--vscode-button-hoverBackground); }
                button.secondary { background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); }
                button.secondary:hover { background: var(--vscode-button-secondaryHoverBackground); }
                button.danger { background: var(--vscode-errorForeground); color: white; }
                
                h2 { margin-top: 0; border-bottom: 1px solid var(--vscode-panel-border); padding-bottom: 10px; margin-bottom: 20px; }
            </style>
        </head>
        <body>
            <div class="sidebar">
                <div style="margin-bottom: 10px; display: flex; justify-content: space-between; align-items: center;">
                    <span style="font-weight: bold;">Connections</span>
                    <button id="btnNew" style="padding: 4px 8px; font-size: 0.9em;">+</button>
                </div>
                <div id="connectionList"></div>
            </div>
            
            <div class="main">
                <h2 id="formTitle">New Connection</h2>
                <div class="form-group">
                    <label for="name">Connection Name</label>
                    <input type="text" id="name" placeholder="e.g. Production, Dev">
                </div>
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
                
                <div class="actions">
                    <button id="btnSave" onclick="save()">Save & Connect</button>
                    <button id="btnDelete" class="danger" onclick="del()" style="display: none;">Delete</button>
                </div>
            </div>

            <script>
                const vscode = acquireVsCodeApi();
                let connections = [];
                let activeName = null;
                let currentEditName = null;

                // Load initial data
                window.addEventListener('message', event => {
                    const message = event.data;
                    switch (message.command) {
                        case 'updateConnections':
                            connections = message.connections;
                            activeName = message.activeName;
                            renderList();
                            break;
                    }
                });
                
                vscode.postMessage({ command: 'loadConnections' });

                document.getElementById('btnNew').addEventListener('click', () => {
                    clearForm();
                });

                function renderList() {
                    const list = document.getElementById('connectionList');
                    list.innerHTML = '';
                    
                    connections.forEach(conn => {
                        const div = document.createElement('div');
                        div.className = 'connection-item';
                        if (conn.name === currentEditName) { // Mark selected in list
                             // Maybe add style for 'editing'
                             div.style.border = '1px solid var(--vscode-focusBorder)';
                        }
                        
                        div.innerHTML = \`<span class="name">\${conn.name}</span>\`;
                        if (conn.name === activeName) {
                            div.innerHTML += \`<span class="status"> (Active)</span>\`;
                        }
                        
                        div.onclick = () => loadForm(conn);
                        list.appendChild(div);
                    });
                }

                function loadForm(conn) {
                    currentEditName = conn.name;
                    document.getElementById('formTitle').innerText = 'Edit Connection';
                    document.getElementById('name').value = conn.name;
                    document.getElementById('host').value = conn.host;
                    document.getElementById('port').value = conn.port;
                    document.getElementById('database').value = conn.database;
                    document.getElementById('user').value = conn.user;
                    document.getElementById('password').value = conn.password || ''; // Password might not be sent back for security? allow update
                    
                    document.getElementById('btnDelete').style.display = 'block';
                    renderList();
                }

                function clearForm() {
                    currentEditName = null;
                    document.getElementById('formTitle').innerText = 'New Connection';
                    document.getElementById('name').value = '';
                    document.getElementById('host').value = '';
                    document.getElementById('port').value = '5480';
                    document.getElementById('database').value = 'system';
                    document.getElementById('user').value = '';
                    document.getElementById('password').value = '';
                    
                    document.getElementById('btnDelete').style.display = 'none';
                    renderList();
                }

                function save() {
                    const name = document.getElementById('name').value;
                    if (!name) {
                        return; // Add validation UI?
                    }
                    
                    const data = {
                        name: name,
                        host: document.getElementById('host').value,
                        port: parseInt(document.getElementById('port').value),
                        database: document.getElementById('database').value,
                        user: document.getElementById('user').value,
                        password: document.getElementById('password').value
                    };
                    
                    vscode.postMessage({
                        command: 'save',
                        data: data
                    });
                }

                function del() {
                    if (currentEditName) {
                        vscode.postMessage({
                            command: 'delete',
                            name: currentEditName
                        });
                    }
                }
            </script>
        </body>
        </html>`}};Ve();var X=B(require("vscode")),ve=class{constructor(e){this._resultsMap=new Map;this._pinnedSources=new Set;this._pinnedResults=new Map;this._resultIdCounter=0;this._extensionUri=e}static{this.viewType="netezza.results"}resolveWebviewView(e,t,s){this._view=e,e.webview.options={enableScripts:!0,localResourceRoots:[X.Uri.joinPath(this._extensionUri,"media")]},e.webview.html=this._getHtmlForWebview(),e.webview.onDidReceiveMessage(n=>{switch(n.command){case"exportCsv":this.exportCsv(n.data);return;case"openInExcel":this.openInExcel(n.data,n.sql);return;case"switchSource":this._activeSourceUri=n.sourceUri,this._updateWebview();return;case"togglePin":this._pinnedSources.has(n.sourceUri)?this._pinnedSources.delete(n.sourceUri):this._pinnedSources.add(n.sourceUri),this._updateWebview();return;case"toggleResultPin":this._toggleResultPin(n.sourceUri,n.resultSetIndex);return;case"switchToPinnedResult":this._switchToPinnedResult(n.resultId);return;case"unpinResult":this._pinnedResults.delete(n.resultId),this._updateWebview();return;case"closeSource":this.closeSource(n.sourceUri);return;case"copyToClipboard":X.env.clipboard.writeText(n.text),X.window.showInformationMessage("Copied to clipboard");return;case"info":X.window.showInformationMessage(n.text);return;case"error":X.window.showErrorMessage(n.text);return}})}setActiveSource(e){this._resultsMap.has(e)&&this._activeSourceUri!==e&&(this._activeSourceUri=e,this._updateWebview())}updateResults(e,t,s=!1){this._resultsMap.has(t)||this._pinnedSources.add(t);let n=[];Array.isArray(e)?n=e:n=[e];let i=this._resultsMap.get(t)||[],a=Array.from(this._pinnedResults.entries()).filter(([p,u])=>u.sourceUri===t).sort((p,u)=>p[1].resultSetIndex-u[1].resultSetIndex),o=[],r=[];a.forEach(([p,u])=>{u.resultSetIndex<i.length&&(o.push(i[u.resultSetIndex]),r.push([p,u]))}),o.push(...n),r.forEach(([p,u],h)=>{let g=this._pinnedResults.get(p);g&&(g.resultSetIndex=h)}),s||Array.from(this._resultsMap.keys()).filter(u=>u!==t&&!this._pinnedSources.has(u)).forEach(u=>{this._resultsMap.delete(u),Array.from(this._pinnedResults.entries()).filter(([g,m])=>m.sourceUri===u).map(([g,m])=>g).forEach(g=>this._pinnedResults.delete(g))}),this._resultsMap.set(t,o),this._activeSourceUri=t,this._view?(this._updateWebview(),this._view.show?.(!0)):X.window.showInformationMessage('Query completed. Please open "Query Results" panel to view data.')}_updateWebview(){this._view&&(this._view.webview.html=this._getHtmlForWebview())}_toggleResultPin(e,t){let s=Array.from(this._pinnedResults.entries()).find(([n,i])=>i.sourceUri===e&&i.resultSetIndex===t);if(s)this._pinnedResults.delete(s[0]);else{let n=`result_${++this._resultIdCounter}`,i=Date.now(),o=`${e.split(/[\\/]/).pop()||e} - Result ${t+1}`;this._pinnedResults.set(n,{sourceUri:e,resultSetIndex:t,timestamp:i,label:o})}this._updateWebview()}_switchToPinnedResult(e){let t=this._pinnedResults.get(e);t&&(this._activeSourceUri=t.sourceUri,this._updateWebview(),this._view&&this._view.webview.postMessage({command:"switchToResultSet",resultSetIndex:t.resultSetIndex}))}async exportCsv(e){let t=await X.window.showSaveDialog({filters:{"CSV Files":["csv"]},saveLabel:"Export"});t&&(await X.workspace.fs.writeFile(t,Buffer.from(e)),X.window.showInformationMessage(`Results exported to ${t.fsPath}`))}async openInExcel(e,t){X.commands.executeCommand("netezza.exportCurrentResultToXlsbAndOpen",e,t)}closeSource(e){if(this._resultsMap.has(e)){if(this._resultsMap.delete(e),this._pinnedSources.delete(e),Array.from(this._pinnedResults.entries()).filter(([s,n])=>n.sourceUri===e).map(([s,n])=>s).forEach(s=>this._pinnedResults.delete(s)),this._activeSourceUri===e){let s=Array.from(this._resultsMap.keys());this._activeSourceUri=s.length>0?s[0]:void 0}this._updateWebview()}}_getHtmlForWebview(){if(!this._view)return"";let{scriptUri:e,virtualUri:t,mainScriptUri:s,styleUri:n,workerUri:i}=this._getScriptUris(),a=this._prepareViewData();return this._buildHtmlDocument(e,t,s,n,a,i)}_getScriptUris(){return{scriptUri:this._view.webview.asWebviewUri(X.Uri.joinPath(this._extensionUri,"media","tanstack-table-core.js")),virtualUri:this._view.webview.asWebviewUri(X.Uri.joinPath(this._extensionUri,"media","tanstack-virtual-core.js")),mainScriptUri:this._view.webview.asWebviewUri(X.Uri.joinPath(this._extensionUri,"media","resultPanel.js")),workerUri:this._view.webview.asWebviewUri(X.Uri.joinPath(this._extensionUri,"media","searchWorker.js")),styleUri:this._view.webview.asWebviewUri(X.Uri.joinPath(this._extensionUri,"media","resultPanel.css"))}}_prepareViewData(){let e=Array.from(this._resultsMap.keys()),t=Array.from(this._pinnedSources),s=Array.from(this._pinnedResults.entries()).map(([o,r])=>({id:o,...r})),n=this._activeSourceUri&&this._resultsMap.has(this._activeSourceUri)?this._activeSourceUri:e.length>0?e[0]:null,i=n?this._resultsMap.get(n):[],a=(o,r)=>typeof r=="bigint"?r>=Number.MIN_SAFE_INTEGER&&r<=Number.MAX_SAFE_INTEGER?Number(r):r.toString():r;return{sourcesJson:JSON.stringify(e),pinnedSourcesJson:JSON.stringify(t),pinnedResultsJson:JSON.stringify(s),activeSourceJson:JSON.stringify(n),resultSetsJson:JSON.stringify(i,a)}}_buildHtmlDocument(e,t,s,n,i,a){let o=this._view.webview.cspSource;return`<!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src ${o} 'unsafe-inline'; worker-src ${o} blob:; connect-src ${o}; style-src ${o} 'unsafe-inline';">
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
                <button onclick="clearAllFilters()" title="Clear all column filters">\u{1F6AB} Clear Filters</button>
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
                let activeGridIndex = window.resultSets && window.resultSets.length > 0 ? window.resultSets.length - 1 : 0;
                const workerUri = "${a}";
            </script>
            <script src="${s}"></script>
            <script>
                // Initialize on load
                init();
            </script>
        </body>
        </html>`}};var $=B(require("vscode"));me();var xe=class{constructor(e,t){this.context=e;this.metadataCache=t}async provideCompletionItems(e,t,s,n){let i=e.getText(),a=this.stripComments(i),o=this.parseLocalDefinitions(a),r=e.lineAt(t).text.substr(0,t.character),p=r.toUpperCase(),u=t.line>0?e.lineAt(t.line-1).text:"",h=u.toUpperCase();if(/(FROM|JOIN)\s+$/.test(p)){let x=await this.getDatabases();return[...o.map(L=>{let O=new $.CompletionItem(L.name,$.CompletionItemKind.Class);return O.detail=L.type,O}),...x]}if(/(?:FROM|JOIN)\s*$/i.test(u)&&/^\s*[a-zA-Z0-9_]*$/.test(r)){let x=await this.getDatabases();return[...o.map(L=>{let O=new $.CompletionItem(L.name,$.CompletionItemKind.Class);return O.detail=L.type,O}),...x]}let g=r.match(/(?:FROM|JOIN)\s+([a-zA-Z0-9_]+)\.\s*$/i);if(g){let x=g[1],M=await this.getSchemas(x);return new $.CompletionList(M,!1)}let m=r.match(/^\s*([a-zA-Z0-9_]+)\.\s*$/i);if(m&&/(?:FROM|JOIN)\s*$/i.test(u)){let x=m[1],M=await this.getSchemas(x);return new $.CompletionList(M,!1)}let b=r.match(/(?:FROM|JOIN)\s+([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\.$/i);if(b){let x=b[1],M=b[2];return this.getTables(x,M)}let T=r.match(/^\s*([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\.\s*$/i);if(T&&/(?:FROM|JOIN)\s*$/i.test(u)){let x=T[1],M=T[2];return this.getTables(x,M)}let R=r.match(/(?:FROM|JOIN)\s+([a-zA-Z0-9_]+)\.\.$/i);if(R){let x=R[1];return this.getTables(x,void 0)}let D=r.match(/^\s*([a-zA-Z0-9_]+)\.\.\s*$/i);if(D&&/(?:FROM|JOIN)\s*$/i.test(u)){let x=D[1];return this.getTables(x,void 0)}if(r.trim().endsWith(".")){let x=r.trim().split(/[\s.]+/),M=r.match(/([a-zA-Z0-9_]+)\.$/);if(M){let L=M[1],O=this.findAlias(a,L);if(O){let Y=o.find(G=>G.name.toUpperCase()===O.table.toUpperCase());return Y?Y.columns.map(G=>{let ce=new $.CompletionItem(G,$.CompletionItemKind.Field);return ce.detail="Local Column",ce}):this.getColumns(O.db,O.schema,O.table)}let W=o.find(Y=>Y.name.toUpperCase()===L.toUpperCase());if(W)return W.columns.map(Y=>{let G=new $.CompletionItem(Y,$.CompletionItemKind.Field);return G.detail="Local Column",G})}}return this.getKeywords()}stripComments(e){let t=e.replace(/--.*$/gm,"");return t=t.replace(/\/\*[\s\S]*?\*\//g,""),t}parseLocalDefinitions(e){let t=[],s=/CREATE\s+TABLE\s+([a-zA-Z0-9_]+)\s+AS\s*\(/gi,n;for(;(n=s.exec(e))!==null;){let o=n[1],r=n.index+n[0].length,p=this.extractBalancedParenthesisContent(e,r);if(p){let u=this.extractColumnsFromQuery(p);t.push({name:o,type:"Temp Table",columns:u})}}let i=/\bWITH\s+/gi;for(;(n=i.exec(e))!==null;){let o=n.index+n[0].length;for(;;){let r=/^\s*([a-zA-Z0-9_]+)\s+AS\s*\(/i,p=e.substring(o),u=p.match(r);if(!u)break;let h=u[1],g=o+u[0].length,m=this.extractBalancedParenthesisContent(e,o+u[0].length-1),b=p.indexOf("(",u.index+u[1].length),T=o+b,R=this.extractBalancedParenthesisContent(e,T+1);if(R){let D=this.extractColumnsFromQuery(R);t.push({name:h,type:"CTE",columns:D}),o=T+1+R.length+1;let x=/^\s*,/,M=e.substring(o);if(x.test(M)){let L=M.match(x);o+=L[0].length}else break}else break}}let a=/\bJOIN\s+\(/gi;for(;(n=a.exec(e))!==null;){let o=n.index+n[0].length,r=this.extractBalancedParenthesisContent(e,o);if(r&&/^\s*SELECT\b/i.test(r)){let p=o+r.length+1,h=e.substring(p).match(/^\s+(?:AS\s+)?([a-zA-Z0-9_]+)/i);if(h){let g=h[1],m=this.extractColumnsFromQuery(r);t.push({name:g,type:"Subquery",columns:m})}}}return t}extractBalancedParenthesisContent(e,t){let s=1,n=t;for(;n<e.length;n++)if(e[n]==="("?s++:e[n]===")"&&s--,s===0)return e.substring(t,n);return null}extractColumnsFromQuery(e){let t=e.match(/^\s*SELECT\s+/i);if(!t)return[];let s="",n=0,i=-1,a=t[0].length;for(let p=a;p<e.length;p++)if(e[p]==="("?n++:e[p]===")"&&n--,n===0&&e.substr(p).match(/^\s+FROM\b/i)){i=p;break}i!==-1?s=e.substring(a,i):s=e.substring(a);let o=[],r="";n=0;for(let p=0;p<s.length;p++){let u=s[p];u==="("?n++:u===")"&&n--,u===","&&n===0?(o.push(r.trim()),r=""):r+=u}return r.trim()&&o.push(r.trim()),o.map(p=>{let u=p.match(/\s+AS\s+([a-zA-Z0-9_]+)$/i);if(u)return u[1];let h=p.match(/\s+([a-zA-Z0-9_]+)$/i);if(h)return h[1];let g=p.split(".");return g[g.length-1]})}getKeywords(){return["SELECT","FROM","WHERE","GROUP BY","ORDER BY","LIMIT","INSERT","INTO","VALUES","UPDATE","SET","DELETE","CREATE","DROP","TABLE","VIEW","DATABASE","JOIN","INNER","LEFT","RIGHT","OUTER","ON","AND","OR","NOT","NULL","IS","IN","BETWEEN","LIKE","AS","DISTINCT","CASE","WHEN","THEN","ELSE","END","WITH","UNION","ALL"].map(t=>{let s=new $.CompletionItem(t,$.CompletionItemKind.Keyword);return s.detail="SQL Keyword",s})}async getDatabases(){let e=this.metadataCache.getDatabases();if(e)return e.map(t=>{if(t instanceof $.CompletionItem)return t;let s=new $.CompletionItem(t.label,t.kind);return s.detail=t.detail,s});try{let s=await U(this.context,"SELECT DATABASE FROM system.._v_database ORDER BY DATABASE",!0);if(!s)return[];let i=JSON.parse(s).map(a=>{let o=new $.CompletionItem(a.DATABASE,$.CompletionItemKind.Module);return o.detail="Database",o});return this.metadataCache.setDatabases(i),i}catch(t){return console.error(t),[]}}async getSchemas(e){let t=this.metadataCache.getSchemas(e);if(t)return t.map(n=>{if(n instanceof $.CompletionItem)return n;let i=new $.CompletionItem(n.label,n.kind);return i.detail=n.detail,i.insertText=n.insertText,i.sortText=n.sortText,i.filterText=n.filterText,i});let s=$.window.setStatusBarMessage(`Fetching schemas for ${e}...`);try{let n=`SELECT SCHEMA FROM ${e}.._V_SCHEMA ORDER BY SCHEMA`,i=await U(this.context,n,!0);if(!i)return[];let o=JSON.parse(i).filter(r=>r.SCHEMA!=null&&r.SCHEMA!=="").map(r=>{let p=r.SCHEMA,u=new $.CompletionItem(p,$.CompletionItemKind.Folder);return u.detail=`Schema in ${e}`,u.insertText=p,u.sortText=p,u.filterText=p,u});return this.metadataCache.setSchemas(e,o),o}catch(n){return console.error("[SqlCompletion] Error in getSchemas:",n),[]}finally{s.dispose()}}async getTables(e,t){let s=t?`${e}.${t}`:`${e}..`,n=this.metadataCache.getTables(s);if(n)return n.map(o=>{if(o instanceof $.CompletionItem)return o;let r=new $.CompletionItem(o.label,o.kind);return r.detail=o.detail,r.sortText=o.sortText,r});let i=t?`Fetching tables for ${e}.${t}...`:`Fetching tables for ${e}...`,a=$.window.setStatusBarMessage(i);try{let o="";t?o=`SELECT OBJNAME, OBJID FROM ${e}.._V_OBJECT_DATA WHERE UPPER(DBNAME) = UPPER('${e}') AND UPPER(SCHEMA) = UPPER('${t}') AND OBJTYPE='TABLE' ORDER BY OBJNAME`:o=`SELECT OBJNAME, OBJID, SCHEMA FROM ${e}.._V_OBJECT_DATA WHERE UPPER(DBNAME) = UPPER('${e}') AND OBJTYPE='TABLE' ORDER BY OBJNAME`;let r=await U(this.context,o,!0);if(!r)return[];let p=JSON.parse(r),u=new Map,h=p.map(g=>{let m=new $.CompletionItem(g.OBJNAME,$.CompletionItemKind.Class);m.detail=t?"Table":`Table (${g.SCHEMA})`,m.sortText=g.OBJNAME;let b=t?`${e}.${t}.${g.OBJNAME}`:`${e}..${g.OBJNAME}`;return u.set(b,g.OBJID),!t&&g.SCHEMA&&u.set(`${e}.${g.SCHEMA}.${g.OBJNAME}`,g.OBJID),m});return this.metadataCache.setTables(s,h,u),h}catch(o){return console.error(o),[]}finally{a.dispose()}}async getColumns(e,t,s){let n,i=e?`${e}..`:"",a=t&&e?`${e}.${t}.${s}`:e?`${e}..${s}`:void 0;a&&(n=this.metadataCache.findTableId(a));let o=`${e||"CURRENT"}.${t||""}.${s}`,r=this.metadataCache.getColumns(o);if(r)return r.map(u=>{if(u instanceof $.CompletionItem)return u;let h=new $.CompletionItem(u.label,u.kind);return h.detail=u.detail,h});let p=$.window.setStatusBarMessage(`Fetching columns for ${s}...`);try{let u="";if(n)u=`SELECT ATTNAME, FORMAT_TYPE FROM ${i}_V_RELATION_COLUMN WHERE OBJID = ${n} ORDER BY ATTNUM`;else{let b=t?`AND UPPER(SCHEMA) = UPPER('${t}')`:"",T=e?`AND UPPER(DBNAME) = UPPER('${e}')`:"";u=`
                    SELECT C.ATTNAME, C.FORMAT_TYPE 
                    FROM ${i}_V_RELATION_COLUMN C
                    JOIN ${i}_V_OBJECT_DATA O ON C.OBJID = O.OBJID
                    WHERE UPPER(O.OBJNAME) = UPPER('${s}') ${b} ${T}
                    ORDER BY C.ATTNUM
                `}let h=await U(this.context,u,!0);if(!h)return[];let m=JSON.parse(h).map(b=>{let T=new $.CompletionItem(b.ATTNAME,$.CompletionItemKind.Field);return T.detail=b.FORMAT_TYPE,T});return this.metadataCache.setColumns(o,m),m}catch(u){return console.error(u),[]}finally{p.dispose()}}findAlias(e,t){let s=new RegExp(`([a-zA-Z0-9_\\.]+) (?:AS\\s+)?${t}\\b`,"gi"),n=new Set(["SELECT","WHERE","GROUP","ORDER","HAVING","LIMIT","ON","AND","OR","NOT","CASE","WHEN","THEN","ELSE","END","JOIN","LEFT","RIGHT","INNER","OUTER","CROSS","FULL","UNION","EXCEPT","INTERSECT","FROM","UPDATE","DELETE","INSERT","INTO","VALUES","SET"]),i;for(;(i=s.exec(e))!==null;){let a=i[1];if(n.has(a.toUpperCase()))continue;let o=a.split(".");return o.length===3?{db:o[0],schema:o[1],table:o[2]}:o.length===2?{table:o[1],schema:o[0]}:{table:o[0]}}return null}};var Oe=B(require("vscode"));me();var ye=class{constructor(e,t,s){this._extensionUri=e;this.context=t;this.metadataCache=s}static{this.viewType="netezza.search"}resolveWebviewView(e,t,s){this._view=e,e.webview.options={enableScripts:!0,localResourceRoots:[this._extensionUri]},e.webview.html=this._getHtmlForWebview(e.webview),e.webview.onDidReceiveMessage(async n=>{switch(n.type){case"search":await this.search(n.value);break;case"navigate":Oe.commands.executeCommand("netezza.revealInSchema",n);break}})}async search(e){if(!e||e.length<2)return;let t=Oe.window.setStatusBarMessage(`$(loading~spin) Searching for "${e}"...`),s=new Set;if(this._view){let o=this.metadataCache.search(e);if(o.length>0){let r=o.map(p=>(s.add(`${p.name}|${p.type}|${p.parent||""}`),{NAME:p.name,SCHEMA:p.schema,DATABASE:p.database,TYPE:p.type,PARENT:p.parent||"",DESCRIPTION:"Result from Cache",MATCH_TYPE:"NAME"}));this._view.webview.postMessage({type:"results",data:r,append:!1})}else this._view.webview.postMessage({type:"results",data:[],append:!1})}let i=`%${e.replace(/'/g,"''").toUpperCase()}%`,a=`
            SELECT OBJNAME AS NAME, SCHEMA, DBNAME AS DATABASE, OBJTYPE AS TYPE, '' AS PARENT, 
                   COALESCE(DESCRIPTION, '') AS DESCRIPTION, 'NAME' AS MATCH_TYPE
            FROM _V_OBJECT_DATA 
            WHERE UPPER(OBJNAME) LIKE '${i}'
            UNION ALL
            SELECT C.ATTNAME AS NAME, O.SCHEMA, O.DBNAME AS DATABASE, 'COLUMN' AS TYPE, O.OBJNAME AS PARENT,
                   COALESCE(C.DESCRIPTION, '') AS DESCRIPTION, 'NAME' AS MATCH_TYPE
            FROM _V_RELATION_COLUMN C
            JOIN _V_OBJECT_DATA O ON C.OBJID = O.OBJID
            WHERE UPPER(C.ATTNAME) LIKE '${i}'
            UNION ALL
            SELECT V.VIEWNAME AS NAME, V.SCHEMA, V.DATABASE, 'VIEW' AS TYPE, '' AS PARENT,
                   'Found in view definition' AS DESCRIPTION, 'DEFINITION' AS MATCH_TYPE
            FROM _V_VIEW V
            WHERE UPPER(V.DEFINITION) LIKE '${i}'
            UNION ALL
            SELECT P.PROCEDURE AS NAME, P.SCHEMA, P.DATABASE, 'PROCEDURE' AS TYPE, '' AS PARENT,
                   'Found in procedure source' AS DESCRIPTION, 'SOURCE' AS MATCH_TYPE
            FROM _V_PROCEDURE P
            WHERE UPPER(P.PROCEDURESOURCE) LIKE '${i}'
            ORDER BY TYPE, NAME
        `;try{let o=await U(this.context,a,!0);if(t.dispose(),this._view&&o){let r=JSON.parse(o);s.size>0&&(r=r.filter(p=>{let u=`${p.NAME}|${p.TYPE}|${p.PARENT||""}`;return!s.has(u)})),r.length>0?this._view.webview.postMessage({type:"results",data:r,append:!0}):s.size}}catch(o){t.dispose(),this._view&&this._view.webview.postMessage({type:"error",message:o.message})}}_getHtmlForWebview(e){return`<!DOCTYPE html>
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
                .cache-badge { background-color: var(--vscode-charts-green); color: white; padding: 1px 4px; border-radius: 2px; font-size: 0.7em; margin-left: 5px; }
                .spinner {
                    border: 2px solid transparent;
                    border-top: 2px solid var(--vscode-progressBar-background); 
                    border-radius: 50%;
                    width: 14px;
                    height: 14px;
                    animation: spin 1s linear infinite;
                    display: inline-block;
                    vertical-align: middle;
                    margin-right: 8px;
                }
                @keyframes spin {
                    0% { transform: rotate(0deg); }
                    100% { transform: rotate(360deg); }
                }
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
                        status.innerHTML = '<span class="spinner"></span> Searching...';
                        // Keep results until new ones come (or clear? we clear inside search handler if necessary or by postMessage)
                        // resultsList.innerHTML = ''; // Don't clear immediately, let the backend drive it
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
                            renderResults(message.data, message.append);
                            break;
                        case 'error':
                            status.textContent = 'Error: ' + message.message;
                            break;
                    }
                });

                function renderResults(data, append) {
                    if (!append) {
                        resultsList.innerHTML = '';
                    }

                    if (!data || data.length === 0) {
                        if (!append && resultsList.children.length === 0) {
                            status.textContent = 'No results found.';
                        }
                        return;
                    }
                    
                    data.forEach(item => {
                        const li = document.createElement('li');
                        li.className = 'result-item';
                        
                        const parentInfo = item.PARENT ? \`Parent: \${item.PARENT}\` : '';
                        const schemaInfo = item.SCHEMA ? \`Schema: \${item.SCHEMA}\` : '';
                        const databaseInfo = item.DATABASE ? \`Database: \${item.DATABASE}\` : '';
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
                                <span>\${databaseInfo}</span>
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
                                database: item.DATABASE,
                                objType: item.TYPE,
                                parent: item.PARENT
                            });
                        });
                        
                        resultsList.appendChild(li);
                    });
                }
            </script>
        </body>
        </html>`}};var re=class{static splitStatements(e){let t=[],s="",n=!1,i=!1,a=!1,o=!1,r=0;for(;r<e.length;){let p=e[r],u=r+1<e.length?e[r+1]:"";if(a)p===`
`&&(a=!1);else if(o){if(p==="*"&&u==="/"){o=!1,s+=p+u,r++,r++;continue}}else if(n)p==="'"&&e[r-1]!=="\\"&&(n=!1);else if(i)p==='"'&&e[r-1]!=="\\"&&(i=!1);else if(p==="-"&&u==="-")a=!0;else if(p==="/"&&u==="*")o=!0;else if(p==="'")n=!0;else if(p==='"')i=!0;else if(p===";"){s.trim()&&t.push(s.trim()),s="",r++;continue}s+=p,r++}return s.trim()&&t.push(s.trim()),t}static getStatementAtPosition(e,t){let s=0,n=e.length,i=!1,a=!1,o=!1,r=!1,p=-1;for(let h=0;h<t;h++){let g=e[h],m=h+1<e.length?e[h+1]:"";o?g===`
`&&(o=!1):r?g==="*"&&m==="/"&&(r=!1,h++):i?g==="'"&&e[h-1]!=="\\"&&(i=!1):a?g==='"'&&e[h-1]!=="\\"&&(a=!1):g==="-"&&m==="-"?o=!0:g==="/"&&m==="*"?r=!0:g==="'"?i=!0:g==='"'?a=!0:g===";"&&(p=h)}s=p+1,i=!1,a=!1,o=!1,r=!1;for(let h=s;h<e.length;h++){let g=e[h],m=h+1<e.length?e[h+1]:"";if(o)g===`
`&&(o=!1);else if(r)g==="*"&&m==="/"&&(r=!1,h++);else if(i)g==="'"&&e[h-1]!=="\\"&&(i=!1);else if(a)g==='"'&&e[h-1]!=="\\"&&(a=!1);else if(g==="-"&&m==="-")o=!0;else if(g==="/"&&m==="*")r=!0;else if(g==="'")i=!0;else if(g==='"')a=!0;else if(g===";"){n=h;break}}let u=e.substring(s,n).trim();return u?{sql:u,start:s,end:n}:null}static getObjectAtPosition(e,t){let s=p=>/[a-zA-Z0-9_."]/i.test(p),n=t;for(;n>0&&s(e[n-1]);)n--;let i=t;for(;i<e.length&&s(e[i]);)i++;let a=e.substring(n,i);if(!a)return null;let o=p=>p?p.replace(/"/g,""):void 0;if(a.includes("..")){let p=a.split("..");if(p.length===2)return{database:o(p[0]),name:o(p[1])}}let r=a.split(".");return r.length===1?{name:o(r[0])}:r.length===2?{schema:o(r[0]),name:o(r[1])}:r.length===3?{database:o(r[0]),schema:o(r[1]),name:o(r[2])}:null}};var he=B(require("vscode"));var $e=class{provideDocumentLinks(e,t){let s=[],n=e.getText(),i=/[a-zA-Z0-9_"]+(\.[a-zA-Z0-9_"]*)+/g,a;for(;(a=i.exec(n))!==null;){let o=e.positionAt(a.index),r=e.positionAt(a.index+a[0].length),p=new he.Range(o,r),u=re.getObjectAtPosition(n,a.index+Math.floor(a[0].length/2));if(u){if(a[0].split(".").length===2&&!u.database&&this.isLikelyAliasReference(n,a.index))continue;let m={name:u.name,schema:u.schema,database:u.database},b=he.Uri.parse(`command:netezza.revealInSchema?${encodeURIComponent(JSON.stringify(m))}`),T=new he.DocumentLink(p,b);T.tooltip=`Reveal ${u.name} in Schema`,s.push(T)}}return s}isLikelyAliasReference(e,t){let n=e.substring(Math.max(0,t-200),t).replace(/--[^\n]*/g,"").replace(/\/\*[\s\S]*?\*\//g,"").toUpperCase();return/(?:FROM|JOIN)\s+[a-zA-Z0-9_"]*$/i.test(n)?!1:!!n.match(/\b(SELECT|WHERE|ON|HAVING|ORDER\s+BY|GROUP\s+BY|AND|OR|SET|VALUES)\b(?!.*\b(?:FROM|JOIN)\b)/)}};var Pe=B(require("vscode")),Le=class{provideFoldingRanges(e,t,s){let n=[],i=[],a=/^\s*--\s*REGION\b/i,o=/^\s*--\s*ENDREGION\b/i;for(let r=0;r<e.lineCount;r++){let u=e.lineAt(r).text;if(a.test(u))i.push(r);else if(o.test(u)&&i.length>0){let h=i.pop();n.push(new Pe.FoldingRange(h,r,Pe.FoldingRangeKind.Region))}}return n}};var J=B(require("vscode"));Ie();var Ce=class{constructor(e,t){this._extensionUri=e;this._context=t}static{this.viewType="netezza.queryHistory"}resolveWebviewView(e,t,s){this._view=e,e.webview.options={enableScripts:!0,localResourceRoots:[this._extensionUri]},e.webview.html=this._getHtmlForWebview(e.webview),this.sendHistoryToWebview(),e.webview.onDidReceiveMessage(async n=>{switch(n.type){case"refresh":this.refresh();break;case"clearAll":await this.clearAllHistory();break;case"deleteEntry":await this.deleteEntry(n.id,n.query);break;case"copyQuery":await J.env.clipboard.writeText(n.query),J.window.showInformationMessage("Query copied to clipboard");break;case"executeQuery":await this.executeQuery(n.query);break;case"getHistory":await this.sendHistoryToWebview();break;case"toggleFavorite":await this.toggleFavorite(n.id);break;case"updateEntry":await this.updateEntry(n.id,n.tags,n.description);break;case"requestEdit":await this.requestEdit(n.id);break;case"requestTagFilter":await this.requestTagFilter(n.tags);break;case"showFavoritesOnly":await this.sendFavoritesToWebview();break;case"filterByTag":await this.sendFilteredByTagToWebview(n.tag);break}})}refresh(){this._view&&this.sendHistoryToWebview()}async sendHistoryToWebview(){if(!this._view)return;let e=new Q(this._context),t=await e.getHistory(),s=await e.getStats();console.log("QueryHistoryView: sending history to webview, entries=",t.length),this._view.webview.postMessage({type:"historyData",history:t,stats:s})}async clearAllHistory(){await J.window.showWarningMessage("Are you sure you want to clear all query history?",{modal:!0},"Clear All")==="Clear All"&&(await new Q(this._context).clearHistory(),this.refresh(),J.window.showInformationMessage("Query history cleared"))}async deleteEntry(e,t){let s=t?`: ${t.substring(0,50)}${t.length>50?"...":""}`:"";await J.window.showWarningMessage(`Are you sure you want to delete this query${s}?`,{modal:!0},"Delete")==="Delete"&&(await new Q(this._context).deleteEntry(e),this.refresh())}async executeQuery(e){let t=await J.workspace.openTextDocument({content:e,language:"sql"});await J.window.showTextDocument(t)}async toggleFavorite(e){await new Q(this._context).toggleFavorite(e),this.refresh()}async updateEntry(e,t,s){await new Q(this._context).updateEntry(e,t,s),this.refresh(),J.window.showInformationMessage("Entry updated successfully")}async requestEdit(e){let n=(await new Q(this._context).getHistory()).find(o=>o.id===e);if(!n){J.window.showErrorMessage("Entry not found");return}let i=await J.window.showInputBox({prompt:"Enter tags (comma separated)",value:n.tags||"",placeHolder:"tag1, tag2, tag3"});if(i===void 0)return;let a=await J.window.showInputBox({prompt:"Enter description",value:n.description||"",placeHolder:"Description for this query"});a!==void 0&&await this.updateEntry(e,i,a)}async requestTagFilter(e){if(e.length===1)await this.sendFilteredByTagToWebview(e[0]);else if(e.length>1){let t=await J.window.showQuickPick(e,{placeHolder:"Filter by which tag?"});t&&await this.sendFilteredByTagToWebview(t)}}async sendFavoritesToWebview(){if(!this._view)return;let e=new Q(this._context),t=await e.getFavorites(),s=await e.getStats();this._view.webview.postMessage({type:"historyData",history:t,stats:s,filter:"favorites"})}async sendFilteredByTagToWebview(e){if(!this._view)return;let t=new Q(this._context),s=await t.getByTag(e),n=await t.getStats();this._view.webview.postMessage({type:"historyData",history:s,stats:n,filter:`tag: ${e}`})}_getHtmlForWebview(e){let t=Lt(),s=e.asWebviewUri(J.Uri.joinPath(this._extensionUri,"media","queryHistory.css")),n=e.asWebviewUri(J.Uri.joinPath(this._extensionUri,"media","queryHistory.js"));return`<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src ${e.cspSource}; script-src ${e.cspSource};">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Query History</title>
    <link href="${s}" rel="stylesheet">
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

    <script src="${n}"></script>
</body>
</html>`}};function Lt(){let l="",e="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";for(let t=0;t<32;t++)l+=e.charAt(Math.floor(Math.random()*e.length));return l}var _e=class{constructor(e){this.context=e;this.schemaCache=new Map;this.tableCache=new Map;this.columnCache=new Map;this.tableIdMap=new Map;this.CACHE_TTL=14400*1e3;this.savePending=!1;this.pendingCacheTypes=new Set;this.loadCacheFromWorkspaceState()}loadCacheFromWorkspaceState(){try{let e=Date.now(),t=this.context.workspaceState.get("sqlCompletion.dbCache");t&&e-t.timestamp<this.CACHE_TTL&&(this.dbCache=t);let s=this.context.workspaceState.get("sqlCompletion.schemaCache");if(s)for(let[o,r]of Object.entries(s))e-r.timestamp<this.CACHE_TTL&&this.schemaCache.set(o,r);let n=this.context.workspaceState.get("sqlCompletion.tableCache");if(n)for(let[o,r]of Object.entries(n))e-r.timestamp<this.CACHE_TTL&&this.tableCache.set(o,r);let i=this.context.workspaceState.get("sqlCompletion.tableIdMap");if(i)for(let[o,r]of Object.entries(i))(this.tableCache.has(o)||e-r.timestamp<this.CACHE_TTL)&&this.tableIdMap.set(o,{data:new Map(Object.entries(r.data)),timestamp:r.timestamp});let a=this.context.workspaceState.get("sqlCompletion.columnCache");if(a)for(let[o,r]of Object.entries(a))e-r.timestamp<this.CACHE_TTL&&this.columnCache.set(o,r)}catch(e){console.error("[MetadataCache] Error loading cache from workspace state:",e)}}scheduleSave(e){this.pendingCacheTypes.add(e),this.savePending||(this.savePending=!0,this.saveTimeoutId=setTimeout(()=>this.flushSave(),1e3))}async flushSave(){this.savePending=!1;let e=new Set(this.pendingCacheTypes);this.pendingCacheTypes.clear();try{if(e.has("db")&&this.dbCache&&await this.context.workspaceState.update("sqlCompletion.dbCache",this.dbCache),e.has("schema")&&this.schemaCache.size>0){let t={};this.schemaCache.forEach((s,n)=>{t[n]=s}),await this.context.workspaceState.update("sqlCompletion.schemaCache",t)}if(e.has("table")&&this.tableCache.size>0){let t={};this.tableCache.forEach((n,i)=>{t[i]=n}),await this.context.workspaceState.update("sqlCompletion.tableCache",t);let s={};this.tableIdMap.forEach((n,i)=>{let a={};n.data.forEach((o,r)=>{a[r]=o}),s[i]={data:a,timestamp:n.timestamp}}),await this.context.workspaceState.update("sqlCompletion.tableIdMap",s)}if(e.has("column")&&this.columnCache.size>0){let t={};this.columnCache.forEach((s,n)=>{t[n]=s}),await this.context.workspaceState.update("sqlCompletion.columnCache",t)}}catch(t){console.error("[MetadataCache] Error saving cache to workspace state:",t)}}async clearCache(){this.saveTimeoutId&&(clearTimeout(this.saveTimeoutId),this.saveTimeoutId=void 0),this.savePending=!1,this.pendingCacheTypes.clear(),this.dbCache=void 0,this.schemaCache.clear(),this.tableCache.clear(),this.columnCache.clear(),this.tableIdMap.clear(),await this.context.workspaceState.update("sqlCompletion.dbCache",void 0),await this.context.workspaceState.update("sqlCompletion.schemaCache",void 0),await this.context.workspaceState.update("sqlCompletion.tableCache",void 0),await this.context.workspaceState.update("sqlCompletion.columnCache",void 0),await this.context.workspaceState.update("sqlCompletion.tableIdMap",void 0)}getDatabases(){return this.dbCache?.data}setDatabases(e){this.dbCache={data:e,timestamp:Date.now()},this.scheduleSave("db")}getSchemas(e){return this.schemaCache.get(e)?.data}setSchemas(e,t){this.schemaCache.set(e,{data:t,timestamp:Date.now()}),this.scheduleSave("schema")}getTables(e){return this.tableCache.get(e)?.data}setTables(e,t,s){let n=Date.now();this.tableCache.set(e,{data:t,timestamp:n}),this.tableIdMap.set(e,{data:s,timestamp:n}),this.scheduleSave("table")}getColumns(e){return this.columnCache.get(e)?.data}setColumns(e,t){this.columnCache.set(e,{data:t,timestamp:Date.now()}),this.scheduleSave("column")}findTableId(e){for(let t of this.tableIdMap.values()){let s=t.data.get(e);if(s!==void 0)return s}}search(e){let t=[],s=e.toLowerCase();for(let[n,i]of this.tableCache){let a=n.split("."),o=a[0],r=a.length>1?a[1]:void 0;for(let p of i.data){let u=typeof p.label=="string"?p.label:p.label.label;u&&u.toLowerCase().includes(s)&&t.push({name:u,type:"TABLE",database:o,schema:r||(p.detail&&p.detail.includes("(")?p.detail.match(/\((.*?)\)/)?.[1]:void 0)})}}for(let[n,i]of this.columnCache){let a=n.split("."),o=a[0],r=a[1],p=a[2];for(let u of i.data){let h=typeof u.label=="string"?u.label:u.label.label;h&&h.toLowerCase().includes(s)&&t.push({name:h,type:"COLUMN",database:o,schema:r,parent:p})}}return t}};var At=B(require("path"));function St(l,e){let t=e.getKeepConnectionOpen();l.text=t?"\u{1F517} Keep Connection ON":"\u26D3\uFE0F\u200D\u{1F4A5} Keep Connection OFF",l.tooltip=t?"Keep Connection Open: ENABLED - Click to disable":"Keep Connection Open: DISABLED - Click to enable",l.backgroundColor=t?new c.ThemeColor("statusBarItem.prominentBackground"):void 0}function tn(l){console.log("Netezza extension: Activating..."),l.subscriptions.push({dispose:()=>{e.closeAllPersistentConnections()}});let e=new ue(l),t=new _e(l),s=new Ee(l,e,t),n=new ve(l.extensionUri),i=c.window.createStatusBarItem(c.StatusBarAlignment.Left,100);i.command="netezza.selectConnectionForTab",i.tooltip="Click to select connection for this SQL tab",l.subscriptions.push(i);let a=()=>{let d=c.window.activeTextEditor;if(d&&d.document.languageId==="sql"){let f=d.document.uri.toString(),w=e.getConnectionForExecution(f);w?(i.text=`$(database) ${w}`,i.show()):(i.text="$(database) Select Connection",i.show())}else i.hide()};a(),e.onDidChangeActiveConnection(a),e.onDidChangeConnections(a),e.onDidChangeDocumentConnection(a),c.window.onDidChangeActiveTextEditor(a);let o=c.window.createStatusBarItem(c.StatusBarAlignment.Right,100);o.command="netezza.toggleKeepConnectionOpen",St(o,e),o.show(),l.subscriptions.push(o),console.log("Netezza extension: Registering SchemaSearchProvider...");let r=new ye(l.extensionUri,l,t);console.log("Netezza extension: Registering QueryHistoryView...");let p=new Ce(l.extensionUri,l),u=c.window.createTreeView("netezza.schema",{treeDataProvider:s,showCollapseAll:!0});l.subscriptions.push(c.window.registerWebviewViewProvider(ve.viewType,n),c.window.registerWebviewViewProvider(ye.viewType,r),c.window.registerWebviewViewProvider(Ce.viewType,p));let h=/(^|\s)(?:[A-Za-z]:\\|\\|\/)?[\w.\-\\\/]+\.py\b|(^|\s)python(?:\.exe)?\s+[^\n]*\.py\b/i;function g(d){return d&&(d.includes(" ")?`"${d.replace(/"/g,'\\"')}"`:d)}function m(d,f,w){let E=/[ \\/]/.test(d)?`& ${g(d)}`:d,y=g(f),S=w.map(v=>g(v)).join(" ");return`${E} ${y}${S?" "+S:""}`.trim()}class b{constructor(){this._onDidChange=new c.EventEmitter;this.onDidChangeCodeLenses=this._onDidChange.event}provideCodeLenses(f){let w=[];for(let C=0;C<f.lineCount;C++){let E=f.lineAt(C);if(h.test(E.text)){let y=E.range,S={title:"Run as script",command:"netezza.runScriptFromLens",arguments:[f.uri,y]};w.push(new c.CodeLens(y,S))}}return w}refresh(){this._onDidChange.fire()}}let T=new b;l.subscriptions.push(c.languages.registerCodeLensProvider({scheme:"file"},T));let R=c.window.createTextEditorDecorationType({backgroundColor:new c.ThemeColor("editor.rangeHighlightBackground"),borderRadius:"3px"});function D(d){let f=d||c.window.activeTextEditor;if(!f)return;let w=f.document,C=[];for(let E=0;E<w.lineCount;E++){let y=w.lineAt(E);h.test(y.text)&&C.push({range:y.range,hoverMessage:"Python script invocation"})}f.setDecorations(R,C)}l.subscriptions.push(c.window.onDidChangeActiveTextEditor(()=>D()),c.workspace.onDidChangeTextDocument(d=>{c.window.activeTextEditor&&d.document===c.window.activeTextEditor.document&&D(c.window.activeTextEditor)})),l.subscriptions.push(c.commands.registerCommand("netezza.runScriptFromLens",async(d,f)=>{try{let w=await c.workspace.openTextDocument(d),C=w.getText(f).trim()||w.lineAt(f.start.line).text.trim();if(!C){c.window.showWarningMessage("No script command found");return}let E=C.split(/\s+/),y=E[0]||"",S=/python(\\.exe)?$/i.test(y)&&E.length>=2&&E[1].toLowerCase().endsWith(".py"),v=y.toLowerCase().endsWith(".py"),N=c.workspace.getConfiguration("netezza").get("pythonPath")||"python",I="";if(S){let z=E[0],H=E[1],q=E.slice(2);I=m(z,H,q)}else if(v){let z=y,H=E.slice(1);I=m(N,z,H)}else I=m(N,"",E);let P=c.window.createTerminal({name:"Netezza: Script"});P.show(!0),P.sendText(I,!0),c.window.showInformationMessage(`Running script: ${I}`)}catch(w){c.window.showErrorMessage(`Error running script: ${w.message}`)}})),D(c.window.activeTextEditor),l.subscriptions.push(c.window.onDidChangeActiveTextEditor(d=>{if(d&&d.document){let f=d.document.uri.toString();n.setActiveSource(f)}})),l.subscriptions.push(c.commands.registerCommand("netezza.toggleKeepConnectionOpen",()=>{let d=e.getKeepConnectionOpen();e.setKeepConnectionOpen(!d),St(o,e);let f=!d;c.window.showInformationMessage(f?"Keep connection open: ENABLED - connection will remain open after queries":"Keep connection open: DISABLED - connection will be closed after each query")}),c.commands.registerCommand("netezza.selectActiveConnection",async()=>{let d=await e.getConnections();if(d.length===0){c.window.showWarningMessage("No connections configured. Please connect first.");return}let f=await c.window.showQuickPick(d.map(w=>w.name),{placeHolder:"Select Active Connection"});f&&(await e.setActiveConnection(f),c.window.showInformationMessage(`Active connection set to: ${f}`))}),c.commands.registerCommand("netezza.selectConnectionForTab",async()=>{let d=c.window.activeTextEditor;if(!d||d.document.languageId!=="sql"){c.window.showWarningMessage("This command is only available for SQL files");return}let f=await e.getConnections();if(f.length===0){c.window.showWarningMessage("No connections configured. Please connect first.");return}let w=d.document.uri.toString(),C=e.getDocumentConnection(w)||e.getActiveConnectionName(),E=f.map(S=>({label:S.name,description:C===S.name?"$(check) Currently selected":`${S.host}:${S.port}/${S.database}`,detail:(C===S.name,void 0),name:S.name})),y=await c.window.showQuickPick(E,{placeHolder:"Select connection for this SQL tab"});y&&(e.setDocumentConnection(w,y.name),c.window.showInformationMessage(`Connection for this tab set to: ${y.name}`))}),c.commands.registerCommand("netezza.openLogin",()=>{Me.createOrShow(l.extensionUri,e)}),c.commands.registerCommand("netezza.refreshSchema",()=>{s.refresh(),c.window.showInformationMessage("Schema refreshed")}),c.commands.registerCommand("netezza.copySelectAll",d=>{if(d&&d.label&&d.dbName&&d.schema){let f=`SELECT * FROM ${d.dbName}.${d.schema}.${d.label} LIMIT 100;`;c.env.clipboard.writeText(f),c.window.showInformationMessage("Copied to clipboard")}}),c.commands.registerCommand("netezza.copyDrop",async d=>{if(d&&d.label&&d.dbName&&d.schema&&d.objType){let f=`${d.dbName}.${d.schema}.${d.label}`,w=`DROP ${d.objType} ${f};`;if(await c.window.showWarningMessage(`Are you sure you want to delete ${d.objType.toLowerCase()} "${f}"?`,{modal:!0},"Yes, delete","Cancel")==="Yes, delete")try{if(!await e.getConnectionString()){c.window.showErrorMessage("No database connection");return}await c.window.withProgress({location:c.ProgressLocation.Notification,title:`Deleting ${d.objType.toLowerCase()} ${f}...`,cancellable:!1},async y=>{await U(l,w,!0,d.connectionName,e)}),c.window.showInformationMessage(`Deleted ${d.objType.toLowerCase()}: ${f}`),s.refresh()}catch(E){c.window.showErrorMessage(`Error during deletion: ${E.message}`)}}}),c.commands.registerCommand("netezza.copyName",d=>{if(d&&d.label&&d.dbName&&d.schema){let f=`${d.dbName}.${d.schema}.${d.label}`;c.env.clipboard.writeText(f),c.window.showInformationMessage("Copied to clipboard")}}),c.commands.registerCommand("netezza.grantPermissions",async d=>{if(d&&d.label&&d.dbName&&d.schema&&d.objType){let f=`${d.dbName}.${d.schema}.${d.label}`,w=await c.window.showQuickPick([{label:"SELECT",description:"Privileges to read data"},{label:"INSERT",description:"Privileges to insert data"},{label:"UPDATE",description:"Privileges to update data"},{label:"DELETE",description:"Privileges to delete data"},{label:"ALL",description:"All privileges (SELECT, INSERT, UPDATE, DELETE)"},{label:"LIST",description:"Privileges to list objects"}],{placeHolder:"Select privilege type"});if(!w)return;let C=await c.window.showInputBox({prompt:"Enter user or group name",placeHolder:"e.g. SOME_USER or GROUP_NAME",validateInput:S=>!S||S.trim().length===0?"User/group name cannot be empty":/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(S.trim())?null:"Invalid user/group name"});if(!C)return;let E=`GRANT ${w.label} ON ${f} TO ${C.trim().toUpperCase()};`;if(await c.window.showInformationMessage(`Execute: ${E}`,{modal:!0},"Yes, execute","Cancel")==="Yes, execute")try{if(!await e.getConnectionString()){c.window.showErrorMessage("No database connection");return}await c.window.withProgress({location:c.ProgressLocation.Notification,title:`Granting ${w.label} on ${f}...`,cancellable:!1},async v=>{await U(l,E,!0,d.connectionName,e)}),c.window.showInformationMessage(`Granted ${w.label} on ${f} to ${C.trim().toUpperCase()}`)}catch(S){c.window.showErrorMessage(`Error granting privileges: ${S.message}`)}}}),c.commands.registerCommand("netezza.groomTable",async d=>{if(d&&d.label&&d.dbName&&d.schema&&d.objType==="TABLE"){let f=`${d.dbName}.${d.schema}.${d.label}`,w=await c.window.showQuickPick([{label:"RECORDS ALL",description:"Groom all records (reclaim space from deleted rows)"},{label:"RECORDS READY",description:"Groom only ready records"},{label:"PAGES ALL",description:"Groom all pages (reorganize data pages)"},{label:"PAGES START",description:"Groom pages from start"},{label:"VERSIONS",description:"Groom versions (clean up old row versions)"}],{placeHolder:"Select GROOM mode"});if(!w)return;let C=await c.window.showQuickPick([{label:"DEFAULT",description:"Use default backupset",value:"DEFAULT"},{label:"NONE",description:"No backupset",value:"NONE"},{label:"Custom",description:"Specify custom backupset ID",value:"CUSTOM"}],{placeHolder:"Select RECLAIM BACKUPSET option"});if(!C)return;let E=C.value;if(C.value==="CUSTOM"){let v=await c.window.showInputBox({prompt:"Enter backupset ID",placeHolder:"np. 12345",validateInput:A=>!A||A.trim().length===0?"Backupset ID cannot be empty":/^\d+$/.test(A.trim())?null:"Backupset ID must be a number"});if(!v)return;E=v.trim()}let y=`GROOM TABLE ${f} ${w.label} RECLAIM BACKUPSET ${E};`;if(await c.window.showWarningMessage(`Execute GROOM on table "${f}"?

${y}

Warning: This operation may be time-consuming for large tables.`,{modal:!0},"Yes, execute","Cancel")==="Yes, execute")try{if(!await e.getConnectionString()){c.window.showErrorMessage("No database connection");return}let A=Date.now();await c.window.withProgress({location:c.ProgressLocation.Notification,title:`GROOM TABLE ${f} (${w.label})...`,cancellable:!1},async I=>{await U(l,y,!0,d.connectionName,e)});let N=((Date.now()-A)/1e3).toFixed(1);c.window.showInformationMessage(`GROOM completed successfully (${N}s): ${f}`)}catch(v){c.window.showErrorMessage(`Error during GROOM: ${v.message}`)}}}),c.commands.registerCommand("netezza.addTableComment",async d=>{if(d&&d.label&&d.dbName&&d.schema&&d.objType==="TABLE"){let f=`${d.dbName}.${d.schema}.${d.label}`,w=await c.window.showInputBox({prompt:"Enter comment for table",placeHolder:"e.g. Table contains customer data",value:d.objectDescription||""});if(w===void 0)return;let C=`COMMENT ON TABLE ${f} IS '${w.replace(/'/g,"''")}';`;try{if(!await e.getConnectionString()){c.window.showErrorMessage("No database connection");return}await U(l,C,!0,d.connectionName,e),c.window.showInformationMessage(`Comment added to table: ${f}`),s.refresh()}catch(E){c.window.showErrorMessage(`Error adding comment: ${E.message}`)}}}),c.commands.registerCommand("netezza.generateStatistics",async d=>{if(d&&d.label&&d.dbName&&d.schema&&d.objType==="TABLE"){let f=`${d.dbName}.${d.schema}.${d.label}`,w=`GENERATE EXPRESS STATISTICS ON ${f};`;if(await c.window.showInformationMessage(`Generate statistics for table "${f}"?

${w}`,{modal:!0},"Yes, generate","Cancel")==="Yes, generate")try{if(!await e.getConnectionString()){c.window.showErrorMessage("No database connection");return}let y=Date.now();await c.window.withProgress({location:c.ProgressLocation.Notification,title:`Generating statistics for ${f}...`,cancellable:!1},async v=>{await U(l,w,!0,d.connectionName,e)});let S=((Date.now()-y)/1e3).toFixed(1);c.window.showInformationMessage(`Statistics generated successfully (${S}s): ${f}`)}catch(E){c.window.showErrorMessage(`Error generating statistics: ${E.message}`)}}}),c.commands.registerCommand("netezza.truncateTable",async d=>{if(d&&d.label&&d.dbName&&d.schema&&d.objType==="TABLE"){let f=`${d.dbName}.${d.schema}.${d.label}`,w=`TRUNCATE TABLE ${f};`;if(await c.window.showWarningMessage(`\u26A0\uFE0F WARNING: Are you sure you want to delete ALL data from the table "${f}"?

${w}

This operation is IRREVERSIBLE!`,{modal:!0},"Yes, delete all data","Cancel")==="Yes, delete all data")try{if(!await e.getConnectionString()){c.window.showErrorMessage("No database connection");return}await c.window.withProgress({location:c.ProgressLocation.Notification,title:`Clearing table ${f}...`,cancellable:!1},async y=>{await U(l,w,!0,d.connectionName,e)}),c.window.showInformationMessage(`Table cleared: ${f}`)}catch(E){c.window.showErrorMessage(`Error clearing table: ${E.message}`)}}}),c.commands.registerCommand("netezza.addPrimaryKey",async d=>{if(d&&d.label&&d.dbName&&d.schema&&d.objType==="TABLE"){let f=`${d.dbName}.${d.schema}.${d.label}`,w=await c.window.showInputBox({prompt:"Enter primary key constraint name",placeHolder:`e.g. PK_${d.label}`,value:`PK_${d.label}`,validateInput:v=>!v||v.trim().length===0?"Constraint name cannot be empty":/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(v.trim())?null:"Invalid constraint name"});if(!w)return;let C=await c.window.showInputBox({prompt:"Enter primary key column names (comma separated)",placeHolder:"e.g. COL1, COL2 or ID",validateInput:v=>!v||v.trim().length===0?"You must provide at least one column":null});if(!C)return;let E=C.split(",").map(v=>v.trim().toUpperCase()).join(", "),y=`ALTER TABLE ${f} ADD CONSTRAINT ${w.trim().toUpperCase()} PRIMARY KEY (${E});`;if(await c.window.showInformationMessage(`Add primary key to table "${f}"?

${y}`,{modal:!0},"Yes, add","Cancel")==="Yes, add")try{if(!await e.getConnectionString()){c.window.showErrorMessage("No database connection");return}await c.window.withProgress({location:c.ProgressLocation.Notification,title:`Adding primary key to ${f}...`,cancellable:!1},async A=>{await U(l,y,!0,d.connectionName,e)}),c.window.showInformationMessage(`Primary key added: ${w.trim().toUpperCase()}`),s.refresh()}catch(v){c.window.showErrorMessage(`Error adding primary key: ${v.message}`)}}}),c.commands.registerCommand("netezza.createDDL",async d=>{try{if(!d||!d.label||!d.dbName||!d.schema||!d.objType){c.window.showErrorMessage("Invalid object selected for DDL generation");return}let f=await e.getConnectionString();if(!f){c.window.showErrorMessage("Connection not configured. Please connect via Netezza: Connect...");return}await c.window.withProgress({location:c.ProgressLocation.Notification,title:`Generating DDL for ${d.objType} ${d.label}...`,cancellable:!1},async()=>{let{generateDDL:w}=await Promise.resolve().then(()=>(gt(),ht)),C=await w(f,d.dbName,d.schema,d.label,d.objType);if(C.success&&C.ddlCode){let E=await c.window.showQuickPick([{label:"Open in Editor",description:"Open DDL code in a new editor",value:"editor"},{label:"Copy to Clipboard",description:"Copy DDL code to clipboard",value:"clipboard"}],{placeHolder:"How would you like to access the DDL code?"});if(E)if(E.value==="editor"){let y=await c.workspace.openTextDocument({content:C.ddlCode,language:"sql"});await c.window.showTextDocument(y),c.window.showInformationMessage(`DDL code generated for ${d.objType} ${d.label}`)}else E.value==="clipboard"&&(await c.env.clipboard.writeText(C.ddlCode),c.window.showInformationMessage("DDL code copied to clipboard"))}else throw new Error(C.error||"DDL generation failed")})}catch(f){c.window.showErrorMessage(`Error generating DDL: ${f.message}`)}}),c.commands.registerCommand("netezza.revealInSchema",async d=>{let f=c.window.setStatusBarMessage(`$(loading~spin) Revealing ${d.name} in schema...`);try{let w=d.connectionName;if(w||(w=e.getActiveConnectionName()),!w){f.dispose(),c.window.showWarningMessage("No active connection. Please select a connection first.");return}if(!await e.getConnectionString(w)){f.dispose(),c.window.showWarningMessage("Not connected to database");return}let E=d.name,y=d.objType,S=y?[y]:["TABLE","VIEW","EXTERNAL TABLE","PROCEDURE","FUNCTION","SEQUENCE","SYNONYM"];if(y==="COLUMN"){if(!d.parent){f.dispose(),c.window.showWarningMessage("Cannot find column without parent table");return}E=d.parent}let v=[];if(d.database)v=[d.database];else{let A=await e.getCurrentDatabase(w);if(A)v=[A];else{let N=await U(l,"SELECT DATABASE FROM system.._v_database ORDER BY DATABASE",!0,w,e);if(!N){f.dispose();return}v=JSON.parse(N).map(P=>P.DATABASE)}}for(let A of v)try{let N=y==="COLUMN"?["TABLE","VIEW","EXTERNAL TABLE"]:S;for(let I of N){let P=`SELECT OBJNAME, OBJTYPE, SCHEMA, OBJID FROM ${A}.._V_OBJECT_DATA WHERE UPPER(OBJNAME) = UPPER('${E.replace(/'/g,"''")}') AND UPPER(OBJTYPE) = UPPER('${I}') AND DBNAME = '${A}'`;d.schema&&(P+=` AND UPPER(SCHEMA) = UPPER('${d.schema.replace(/'/g,"''")}')`);let z=await U(l,P,!0,w,e);if(z){let H=JSON.parse(z);if(H.length>0){let q=H[0],{SchemaItem:K}=await Promise.resolve().then(()=>(Ve(),ut)),de=new K(q.OBJNAME,c.TreeItemCollapsibleState.Collapsed,`netezza:${q.OBJTYPE}`,A,q.OBJTYPE,q.SCHEMA,q.OBJID,void 0,w);await u.reveal(de,{select:!0,focus:!0,expand:!0}),f.dispose(),c.window.setStatusBarMessage(`$(check) Found ${E} in ${A}.${q.SCHEMA}`,3e3);return}}}}catch(N){console.log(`Error searching in ${A}:`,N)}f.dispose(),c.window.showWarningMessage(`Could not find ${y||"object"} ${E}`)}catch(w){f.dispose(),c.window.showErrorMessage(`Error revealing item: ${w.message}`)}}),c.commands.registerCommand("netezza.showQueryHistory",()=>{c.commands.executeCommand("netezza.queryHistory.focus")}),c.commands.registerCommand("netezza.clearQueryHistory",async()=>{let{QueryHistoryManager:d}=await Promise.resolve().then(()=>(Ie(),nt)),f=new d(l);await c.window.showWarningMessage("Are you sure you want to clear all query history?",{modal:!0},"Clear All")==="Clear All"&&(await f.clearHistory(),p.refresh(),c.window.showInformationMessage("Query history cleared"))})),l.subscriptions.push(c.languages.registerDocumentLinkProvider({language:"sql"},new $e)),l.subscriptions.push(c.languages.registerFoldingRangeProvider({language:"sql"},new Le)),l.subscriptions.push(c.commands.registerCommand("netezza.jumpToSchema",async()=>{let d=c.window.activeTextEditor;if(!d)return;let f=d.document,w=d.selection,C=f.offsetAt(w.active),E=re.getObjectAtPosition(f.getText(),C);E?c.commands.executeCommand("netezza.revealInSchema",E):c.window.showWarningMessage("No object found at cursor")}));let x=c.commands.registerCommand("netezza.runQuery",async()=>{let d=c.window.activeTextEditor;if(!d){c.window.showErrorMessage("No active editor found");return}let f=d.document,w=d.selection,C=f.getText(),E=f.uri.toString(),y=[];if(w.isEmpty){let v=f.offsetAt(w.active),A=re.getStatementAtPosition(C,v);if(A){y=[A.sql];let N=f.positionAt(A.start),I=f.positionAt(A.end);d.selection=new c.Selection(N,I)}else{c.window.showWarningMessage("No SQL statement found at cursor");return}}else{let v=f.getText(w);if(!v.trim()){c.window.showWarningMessage("No SQL query selected");return}y=re.splitStatements(v)}if(y.length===0)return;let S=y.length===1?y[0].trim():null;if(S){let v=S.split(/\s+/),A=v[0]||"",N=/python(\.exe)?$/i.test(A)&&v.length>=2&&v[1].toLowerCase().endsWith(".py"),I=A.toLowerCase().endsWith(".py");if(N||I){let z=c.workspace.getConfiguration("netezza").get("pythonPath")||"python",H="";if(N){let K=v[0],de=v[1],Nt=v.slice(2);H=m(K,de,Nt)}else{let K=A,de=v.slice(1);H=m(z,K,de)}let q=c.window.createTerminal({name:"Netezza: Script"});q.show(!0),q.sendText(H,!0),c.window.showInformationMessage(`Running script: ${H}`);return}}try{let v=await qe(l,y,e,E);n.updateResults(v,E,!1),c.commands.executeCommand("netezza.results.focus")}catch(v){c.window.showErrorMessage(`Error executing query: ${v.message}`)}}),M=c.commands.registerCommand("netezza.runQueryBatch",async()=>{let d=c.window.activeTextEditor;if(!d){c.window.showErrorMessage("No active editor found");return}let f=d.document,w=d.selection,C=f.uri.toString(),E;if(w.isEmpty?E=f.getText():E=f.getText(w),!E.trim()){c.window.showWarningMessage("No SQL query to execute");return}let S=E.trim().split(/\s+/),v=S[0]||"",A=/python(\.exe)?$/i.test(v)&&S.length>=2&&S[1].toLowerCase().endsWith(".py"),N=v.toLowerCase().endsWith(".py");if(A||N){let P=c.workspace.getConfiguration("netezza").get("pythonPath")||"python",z="";if(A){let q=S[0],K=S[1],de=S.slice(2);z=m(q,K,de)}else{let q=v,K=S.slice(1);z=m(P,q,K)}let H=c.window.createTerminal({name:"Netezza: Script"});H.show(!0),H.sendText(z,!0),c.window.showInformationMessage(`Running script: ${z}`);return}try{let{runQueryRaw:I}=await Promise.resolve().then(()=>(me(),dt)),P=await I(l,E,!1,e,void 0,C);P&&(n.updateResults([P],C,!1),c.commands.executeCommand("netezza.results.focus"))}catch(I){c.window.showErrorMessage(`Error executing query: ${I.message}`)}});l.subscriptions.push(M);let L=c.window.createOutputChannel("Netezza"),O=(d,f)=>{let w=Date.now()-f;L.appendLine(`[${new Date().toLocaleTimeString()}] ${d} completed in ${w}ms`),L.show(!0)},W=c.commands.registerCommand("netezza.exportToXlsb",async()=>{let d=c.window.activeTextEditor;if(!d){c.window.showErrorMessage("No active editor found");return}let f=d.selection,w=f.isEmpty?d.document.getText():d.document.getText(f);if(!w.trim()){c.window.showWarningMessage("No SQL query to export");return}let C=await c.window.showSaveDialog({filters:{"Excel Workbook":["xlsx"]},saveLabel:"Export to XLSX"});if(!C)return;let E=Date.now();try{let y=d.document.uri.toString(),S=e.getConnectionForExecution(y),v=await e.getConnectionString(S);if(!v)throw new Error("Connection not configured. Please connect via Netezza: Connect...");await c.window.withProgress({location:c.ProgressLocation.Notification,title:"Exporting to XLSX...",cancellable:!1},async A=>{let{exportQueryToXlsb:N}=await Promise.resolve().then(()=>(Te(),be)),I=await N(v,w,C.fsPath,!1,P=>{A.report({message:P}),L.appendLine(`[XLSX Export] ${P}`)});if(!I.success)throw new Error(I.message)}),O("Export to XLSX",E),c.window.showInformationMessage(`Results exported to ${C.fsPath}`)}catch(y){c.window.showErrorMessage(`Error exporting to XLSX: ${y.message}`)}}),Y=c.commands.registerCommand("netezza.exportToCsv",async()=>{let d=c.window.activeTextEditor;if(!d){c.window.showErrorMessage("No active editor found");return}let f=d.selection,w=f.isEmpty?d.document.getText():d.document.getText(f);if(!w.trim()){c.window.showWarningMessage("No SQL query to export");return}let C=await c.window.showSaveDialog({filters:{"CSV Files":["csv"]},saveLabel:"Export to CSV"});if(!C)return;let E=Date.now();try{let y=d.document.uri.toString(),S=e.getConnectionForExecution(y),v=await e.getConnectionString(S);if(!v)throw new Error("Connection not configured. Please connect via Netezza: Connect...");await c.window.withProgress({location:c.ProgressLocation.Notification,title:"Exporting to CSV...",cancellable:!1},async A=>{let{exportToCsv:N}=await Promise.resolve().then(()=>(vt(),Et));await N(l,v,w,C.fsPath,A)}),O("Export to CSV",E),c.window.showInformationMessage(`Results exported to ${C.fsPath}`)}catch(y){c.window.showErrorMessage(`Error exporting to CSV: ${y.message}`)}}),G=c.commands.registerCommand("netezza.copyXlsbToClipboard",async()=>{let d=c.window.activeTextEditor;if(!d){c.window.showErrorMessage("No active editor found");return}let f=d.selection,w=f.isEmpty?d.document.getText():d.document.getText(f);if(!w.trim()){c.window.showWarningMessage("No SQL query to export");return}try{let C=d.document.uri.toString(),E=e.getConnectionForExecution(C),y=await e.getConnectionString(E);if(!y)throw new Error("Connection not configured. Please connect via Netezza: Connect...");let S=Date.now();if(await c.window.withProgress({location:c.ProgressLocation.Notification,title:"Exporting to XLSX and copying to clipboard...",cancellable:!1},async A=>{let{exportQueryToXlsb:N,getTempFilePath:I}=await Promise.resolve().then(()=>(Te(),be)),P=I(),z=await N(y,w,P,!0,H=>{A.report({message:H}),L.appendLine(`[XLSX Clipboard] ${H}`)});if(!z.success)throw new Error(z.message);if(!z.details?.clipboard_success)throw new Error("Failed to copy file to clipboard")}),O("Copy XLSX to Clipboard",S),await c.window.showInformationMessage("Excel file copied to clipboard! You can now paste it into Excel or Windows Explorer.","Show Temp Folder","OK")==="Show Temp Folder"){let A=require("os").tmpdir();await c.env.openExternal(c.Uri.file(A))}}catch(C){c.window.showErrorMessage(`Error copying XLSX to clipboard: ${C.message}`)}}),ce=c.commands.registerCommand("netezza.exportToXlsbAndOpen",async()=>{let d=c.window.activeTextEditor;if(!d){c.window.showErrorMessage("No active editor found");return}let f=d.selection,w=f.isEmpty?d.document.getText():d.document.getText(f);if(!w.trim()){c.window.showWarningMessage("No SQL query to export");return}let C=await c.window.showSaveDialog({filters:{"Excel Workbook":["xlsx"]},saveLabel:"Export to XLSX and Open"});if(!C)return;let E=Date.now();try{let y=d.document.uri.toString(),S=e.getConnectionForExecution(y),v=await e.getConnectionString(S);if(!v)throw new Error("Connection not configured. Please connect via Netezza: Connect...");await c.window.withProgress({location:c.ProgressLocation.Notification,title:"Exporting to XLSX and opening...",cancellable:!1},async A=>{let{exportQueryToXlsb:N}=await Promise.resolve().then(()=>(Te(),be)),I=await N(v,w,C.fsPath,!1,P=>{A.report({message:P}),L.appendLine(`[XLSX Export] ${P}`)});if(!I.success)throw new Error(I.message)}),O("Export to XLSX and Open",E),await c.env.openExternal(C),c.window.showInformationMessage(`Results exported and opened: ${C.fsPath}`)}catch(y){c.window.showErrorMessage(`Error exporting to XLSX: ${y.message}`)}}),Fe=c.commands.registerCommand("netezza.importClipboard",async()=>{try{let f=c.window.activeTextEditor?.document?.uri?.toString(),w=e.getConnectionForExecution(f),C=await e.getConnectionString(w);if(!C)throw new Error("Connection not configured. Please connect via Netezza: Connect...");let E=await c.window.showInputBox({prompt:"Enter target table name (leave empty for auto-generated name)",placeHolder:"e.g. my_schema.my_table or leave empty",validateInput:A=>!A||A.trim().length===0||/^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?(\.[a-zA-Z_][a-zA-Z0-9_]*)?$/.test(A.trim())?null:"Invalid table name format. Use: [database.]schema.table"});if(E===void 0)return;let y;if(!E||E.trim().length===0)try{let N=await U(l,"SELECT CURRENT_CATALOG, CURRENT_SCHEMA",!0);if(N){let I=JSON.parse(N);if(I&&I.length>0){let P=I[0].CURRENT_CATALOG||"SYSTEM",z=I[0].CURRENT_SCHEMA||"ADMIN",q=new Date().toISOString().slice(0,10).replace(/-/g,""),K=Math.floor(Math.random()*1e4).toString().padStart(4,"0");y=`${P}.${z}.IMPORT_${q}_${K}`,c.window.showInformationMessage(`Auto-generated table name: ${y}`)}else throw new Error("Could not determine current database/schema")}else throw new Error("Could not determine current database/schema")}catch(A){c.window.showErrorMessage(`Error getting current database/schema: ${A.message}`);return}else y=E.trim();let S=await c.window.showQuickPick([{label:"Auto-detect",description:"Automatically detect clipboard format (text or Excel XML)",value:null},{label:"Excel XML Spreadsheet",description:"Force Excel XML format processing",value:"XML Spreadsheet"},{label:"Plain Text",description:"Force plain text processing with delimiter detection",value:"TEXT"}],{placeHolder:"Select clipboard data format"});if(!S)return;let v=Date.now();await c.window.withProgress({location:c.ProgressLocation.Notification,title:"Importing clipboard data...",cancellable:!1},async A=>{let{importClipboardDataToNetezza:N}=await Promise.resolve().then(()=>(Tt(),bt)),I=await N(y,C,S.value,{},P=>{A.report({message:P}),L.appendLine(`[Clipboard Import] ${P}`)});if(!I.success)throw new Error(I.message);I.details&&(L.appendLine(`[Clipboard Import] Rows processed: ${I.details.rowsProcessed}`),L.appendLine(`[Clipboard Import] Columns: ${I.details.columns}`),L.appendLine(`[Clipboard Import] Format: ${I.details.format}`))}),O("Import Clipboard Data",v),c.window.showInformationMessage(`Clipboard data imported successfully to table: ${y}`)}catch(d){c.window.showErrorMessage(`Error importing clipboard data: ${d.message}`)}}),k=c.commands.registerCommand("netezza.importData",async()=>{try{let d=await e.getConnectionString();if(!d)throw new Error("Connection not configured. Please connect via Netezza: Connect...");let f=await c.window.showOpenDialog({canSelectFiles:!0,canSelectFolders:!1,canSelectMany:!1,filters:{"Data Files":["csv","txt","xlsx","xlsb","json"],"CSV Files":["csv"],"Excel Files":["xlsx","xlsb"],"Text Files":["txt"],"JSON Files":["json"],"All Files":["*"]},openLabel:"Select file to import"});if(!f||f.length===0)return;let w=f[0].fsPath,C=await c.window.showInputBox({prompt:"Enter target table name (leave empty for auto-generated name)",placeHolder:"e.g. my_schema.my_table or leave empty",validateInput:v=>!v||v.trim().length===0||/^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?(\.[a-zA-Z_][a-zA-Z0-9_]*)?$/.test(v.trim())?null:"Invalid table name format. Use: [database.]schema.table"});if(C===void 0)return;let E;if(!C||C.trim().length===0)try{let A=await U(l,"SELECT CURRENT_CATALOG, CURRENT_SCHEMA",!0);if(A){let N=JSON.parse(A);if(N&&N.length>0){let I=N[0].CURRENT_CATALOG||"SYSTEM",P=N[0].CURRENT_SCHEMA||"ADMIN",H=new Date().toISOString().slice(0,10).replace(/-/g,""),q=Math.floor(Math.random()*1e4).toString().padStart(4,"0");E=`${I}.${P}.IMPORT_${H}_${q}`,c.window.showInformationMessage(`Auto-generated table name: ${E}`)}else throw new Error("Could not determine current database/schema")}else throw new Error("Could not determine current database/schema")}catch(v){c.window.showErrorMessage(`Error getting current database/schema: ${v.message}`);return}else E=C.trim();let y=await c.window.showQuickPick([{label:"Default Import",description:"Use default settings",value:{}},{label:"Custom Options",description:"Configure import settings (coming soon)",value:null}],{placeHolder:"Select import options"});if(!y)return;if(y.value===null){c.window.showInformationMessage("Custom options will be available in future version");return}let S=Date.now();await c.window.withProgress({location:c.ProgressLocation.Notification,title:"Importing data...",cancellable:!1},async v=>{let{importDataToNetezza:A}=await Promise.resolve().then(()=>(Ke(),yt)),N=await A(w,E,d,y.value||{},I=>{v.report({message:I}),L.appendLine(`[Import] ${I}`)});if(!N.success)throw new Error(N.message);N.details&&(L.appendLine(`[Import] Rows processed: ${N.details.rowsProcessed}`),L.appendLine(`[Import] Columns: ${N.details.columns}`),L.appendLine(`[Import] Delimiter: ${N.details.detectedDelimiter}`))}),O("Import Data",S),c.window.showInformationMessage(`Data imported successfully to table: ${E}`)}catch(d){c.window.showErrorMessage(`Error importing data: ${d.message}`)}}),le=c.commands.registerCommand("netezza.exportCurrentResultToXlsbAndOpen",async(d,f)=>{try{if(!d){c.window.showErrorMessage("No data to export");return}let w=require("os"),C=require("path"),E=new Date().toISOString().replace(/[:.]/g,"-"),y=C.join(w.tmpdir(),`netezza_results_${E}.xlsx`),S=Date.now();await c.window.withProgress({location:c.ProgressLocation.Notification,title:"Creating Excel file...",cancellable:!1},async A=>{let{exportCsvToXlsb:N}=await Promise.resolve().then(()=>(Te(),be)),I=await N(d,y,!1,{source:"Query Results Panel",sql:f},P=>{A.report({message:P}),L.appendLine(`[CSV to XLSX] ${P}`)});if(!I.success)throw new Error(I.message)});let v=Date.now()-S;L.appendLine(`[${new Date().toLocaleTimeString()}] Export Current Result to Excel completed in ${v}ms`),await c.env.openExternal(c.Uri.file(y)),c.window.showInformationMessage(`Results exported and opened: ${y}`)}catch(w){c.window.showErrorMessage(`Error exporting to Excel: ${w.message}`)}});l.subscriptions.push(x),l.subscriptions.push(W),l.subscriptions.push(Y),l.subscriptions.push(G),l.subscriptions.push(ce),l.subscriptions.push(le),l.subscriptions.push(Fe),l.subscriptions.push(k);let te=c.workspace.onWillSaveTextDocument(async d=>{}),ke=c.commands.registerCommand("netezza.smartPaste",async()=>{try{let d=c.window.activeTextEditor;if(!d)return;let w=c.workspace.getConfiguration("netezza").get("pythonPath")||"python",C=At.join(l.extensionPath,"python","check_clipboard_format.py"),E=require("child_process");if(await new Promise(S=>{let v=E.spawn(w,[C]);v.on("close",A=>{S(A===1)}),v.on("error",()=>{S(!1)})})){let S=await c.window.showQuickPick([{label:"\u{1F4CA} import to Netezza table",description:'Detected "XML Spreadsheet" format - import data to database',value:"import"},{label:"\u{1F4DD} Paste as text",description:"Paste clipboard content as plain text",value:"paste"}],{placeHolder:'Detected "XML Spreadsheet" format in clipboard - choose an action'});if(S?.value==="import")c.commands.executeCommand("netezza.importClipboard");else if(S?.value==="paste"){let v=await c.env.clipboard.readText(),A=d.selection;await d.edit(N=>{N.replace(A,v)})}}else{let S=await c.env.clipboard.readText(),v=d.selection;await d.edit(A=>{A.replace(v,S)})}}catch(d){c.window.showErrorMessage(`Error during paste: ${d.message}`)}}),He=c.workspace.onDidChangeTextDocument(async d=>{if(d.document.languageId!=="sql"&&d.document.languageId!=="mssql"||d.contentChanges.length!==1)return;let f=d.contentChanges[0];if(f.text!==" ")return;let w=c.window.activeTextEditor;if(!w||w.document!==d.document)return;let E=d.document.lineAt(f.range.start.line).text,y=new Map([["SX","SELECT"],["WX","WHERE"],["GX","GROUP BY"],["HX","HAVING"],["OX","ORDER BY"],["FX","FROM"],["JX","JOIN"],["LX","LIMIT"],["IX","INSERT INTO"],["UX","UPDATE"],["DX","DELETE FROM"],["CX","CREATE TABLE"]]);for(let[S,v]of y)if(new RegExp(`\\b${S}\\s$`,"i").test(E)){let N=E.toUpperCase().lastIndexOf(S.toUpperCase());if(N>=0){let I=new c.Position(f.range.start.line,N),P=new c.Position(f.range.start.line,N+S.length+1);await w.edit(z=>{z.replace(new c.Range(I,P),v+" ")}),["SELECT","FROM","JOIN"].includes(v)&&setTimeout(()=>{c.commands.executeCommand("editor.action.triggerSuggest")},100);break}}});l.subscriptions.push(ke),l.subscriptions.push(He);let Rt=new xe(l,t);l.subscriptions.push(c.languages.registerCompletionItemProvider(["sql","mssql"],Rt,"."," ")),l.subscriptions.push(c.commands.registerCommand("netezza.clearAutocompleteCache",async()=>{await c.window.showWarningMessage("Are you sure you want to clear the autocomplete cache? This will remove all cached databases, schemas, tables, and columns.",{modal:!0},"Clear Cache")==="Clear Cache"&&(await t.clearCache(),c.window.showInformationMessage("Autocomplete cache cleared successfully. Cache will be rebuilt on next use."))}))}function nn(){}0&&(module.exports={activate,deactivate});
