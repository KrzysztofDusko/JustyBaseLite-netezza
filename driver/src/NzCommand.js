
class NzCommand {
    constructor(connection) {
        this.connection = connection;
        this.commandText = '';
        this.parameters = [];
        this._recordsAffected = -1;
        this.commandTimeout = connection.commandTimeout !== undefined ? connection.commandTimeout : 30; // Default 30s, 0 = no timeout
    }

    async execute() {
        return this.connection.execute(this, false);
    }

    async executeNonQuery() {
        await this.connection.execute(this, false);
        return this._recordsAffected;
    }

    async executeReader() {
        return this.connection.executeReader(this);
    }

    async cancel() {
        return this.connection.cancel();
    }
}

module.exports = NzCommand;
