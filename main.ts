import { Database, aql } from 'arangojs';

// neo4j와 mysql의 성능을 테스트할 코드를 작성하고 싶다.
import mysql from 'mysql2/promise';
import neo4j from 'neo4j-driver';
import { v4 as uuidv4 } from 'uuid';

const treeDepth = 2;

function createTree(depth: number): TreeNode {
        if (depth === 0) return new TreeNode();

        let node = new TreeNode();
        let childCount = Math.floor(Math.random() * 7) + 1; // 1~10의 랜덤 자식 노드 수

        for (let i = 0; i < childCount; i++) {
            node.children.push(createTree(depth - 1));
        }

        return node;
    }

interface DB{
    insertData: (tree: TreeNode) => Promise<void>;
    deleteAll: () => Promise<void>;
    execute: () => Promise<number>;
}

class TreeNode {
    id: string;
    children: TreeNode[];

    constructor() {
        this.id = uuidv4();
        this.children = [];
    }
}

class Mysql implements DB{
    private connection!: mysql.Connection;

    constructor(private id: string, private pw: string, private database: string) {
        
    }

    async init() {
        this.connection = await mysql.createConnection({
            host: 'localhost',
            user: this.id,
            password: this.pw,
            database: this.database
        });

        // Create Nodes table
        let query = `CREATE TABLE IF NOT EXISTS Nodes (
            id VARCHAR(255) PRIMARY KEY
        )`;
        await this.connection.query(query);

        // Create ClosureTable
        query = `CREATE TABLE IF NOT EXISTS ClosureTable (
            ancestor VARCHAR(255),
            descendant VARCHAR(255),
            PRIMARY KEY (ancestor, descendant),
            FOREIGN KEY (ancestor) REFERENCES Nodes(id),
            FOREIGN KEY (descendant) REFERENCES Nodes(id)
        )`;
        await this.connection.query(query);
    }

    private async insertNodes(tree: TreeNode) {
        let nodes: TreeNode[] = [];
        let relations: { parentId: string, childId: string }[] = [];

        let stack: { node: TreeNode, parentId: string | null }[] = [{ node: tree, parentId: null }];

        while (stack.length > 0) {
            let { node, parentId } = stack.pop()!;
            nodes.push(node);

            if (parentId) {
                relations.push({ parentId, childId: node.id });
            }

            for (let child of node.children) {
                stack.push({ node: child, parentId: node.id });
            }
        }

        let nodeValues = nodes.map(node => `('${node.id}')`).join(', ');
        let query = `INSERT INTO Nodes (id) VALUES ${nodeValues}`;
        await this.connection.query(query);

        let relationValues = relations.map(relation => `('${relation.parentId}', '${relation.childId}')`).join(', ');
        query = `INSERT INTO ClosureTable (ancestor, descendant) VALUES ${relationValues}`;
        await this.connection.query(query);
    }

    async insertData(tree: TreeNode) {
        await this.connection.query(`set foreign_key_checks=0`);
        await this.insertNodes(tree);
        await this.connection.query(`set foreign_key_checks=1`);
    }

    async deleteAll() {
        await this.connection.query('DELETE FROM ClosureTable');
        await this.connection.query('DELETE FROM Nodes');
    }

    async execute() {
        const startTime = Date.now();

        let joinQuery = '';
        for (let i = 1; i < treeDepth; i++) {
            joinQuery += `JOIN ClosureTable c${i+1} ON c${i}.descendant = c${i+1}.ancestor `;
        }

        // @ts-ignore
        let [rows]: [mysql.RowDataPacket[]] = await this.connection.query('SELECT id FROM Nodes ORDER BY id LIMIT 1');
        // @ts-ignore
        let rootNodeId = rows[0].id;

        let query = `
            SELECT c${treeDepth}.descendant 
            FROM ClosureTable c1
            ${joinQuery}
            WHERE c1.ancestor = '${rootNodeId}'
        `;

        // @ts-ignore
        [rows] = await this.connection.query(query);
        const endTime = Date.now();
        return endTime - startTime; // 실행 시간을 밀리초 단위로 반환
    }
}

class Neo4j implements DB{
    private driver! : any;
    private session! : any;

    constructor(private id: string, private pw: string, private database: string) {
        
    }

    async init() {
        this.driver = neo4j.driver(
            'neo4j://localhost',
            neo4j.auth.basic(this.id, this.pw)
        );

        this.session = this.driver.session({
            defaultAccessMode: neo4j.session.WRITE
        });
    }

    private async insertNode(node: TreeNode, parentId: string | null = null, tx: any) {
        let query = parentId 
            ? `CREATE (n:Node {id: '${node.id}'})<-[:CHILD]-(p:Node {id: '${parentId}'})`
            : `CREATE (n:Node {id: '${node.id}'})`;

        await tx.run(query);

        for (let child of node.children) {
            await this.insertNode(child, node.id, tx);
        }
    }

    async insertData(tree: TreeNode) {
        const tx = this.session.beginTransaction();
        try {
            await this.insertNode(tree, null, tx);
            await tx.commit();
        } catch (error) {
            await tx.rollback();
            throw error;
        }
    }

    async deleteAll() {
        await this.session.run('MATCH (n) DETACH DELETE n');
    }

    async execute() {
        const startTime = Date.now();

        let rootNodeId = (await this.session.run('MATCH (n:Node) RETURN n.id ORDER BY n.id LIMIT 1')).records[0].get('n.id');

        let query = `MATCH p=(root:Node {id: '${rootNodeId}'})-[:CHILD*${treeDepth}]->(leaf:Node) RETURN leaf.id`;

        let result = await this.session.run(query);
        const endTime = Date.now();
        return endTime - startTime; // 실행 시간을 밀리초 단위로 반환
    }
}

class Arango implements DB {
    private db!: Database;

    constructor(private id: string, private pw: string, private database: string) {
        
    }

    async init() {
        let db = new Database({
            url: 'http://localhost:8529',
            auth: { username: this.id, password: this.pw },
        });

        // Create database if not exists
        if (!await db.listDatabases().then(dbs => dbs.includes(this.database))) {
            await db.createDatabase(this.database);
        }

        // Now that we're sure the database exists, specify the databaseName
        this.db = db.database(this.database);

        // Create collection if not exists
        const nodesCollection = this.db.collection('Nodes');
        if (!await nodesCollection.exists()) {
            await nodesCollection.create();
        }

        const edgesCollection = this.db.collection('Edges');
        if (!await edgesCollection.exists()) {
            await edgesCollection.create();
        }
    }

    private async insertNodes(tree: TreeNode) {
        const nodesCollection = this.db.collection('Nodes');
        const edgesCollection = this.db.collection('Edges');

        let stack: { node: TreeNode, parentId: string | null }[] = [{ node: tree, parentId: null }];

        while (stack.length > 0) {
            let { node, parentId } = stack.pop()!;
            await nodesCollection.save({ _key: node.id });

            if (parentId) {
                await edgesCollection.save({ _from: `Nodes/${parentId}`, _to: `Nodes/${node.id}` });
            }

            for (let child of node.children) {
                stack.push({ node: child, parentId: node.id });
            }
        }
    }

    async insertData(tree: TreeNode) {
        await this.insertNodes(tree);
    }

    async deleteAll() {
        const nodesCollection = this.db.collection('Nodes');
        const edgesCollection = this.db.collection('Edges');

        if (await nodesCollection.exists()) {
            await nodesCollection.truncate();
        }

        if (await edgesCollection.exists()) {
            await edgesCollection.truncate();
        }
    }

    async execute() {
        const startTime = Date.now();

        const rootNodeId = (await this.db.query(aql`
            FOR node IN Nodes
            SORT node._key
            LIMIT 1
            RETURN node._key
        `)).next();

        const query = aql`
            FOR v IN 1..${aql.literal(treeDepth)} OUTBOUND ${aql.literal(rootNodeId)} Edges
            RETURN v._key
        `;

        await this.db.query(query);
        const endTime = Date.now();
        return endTime - startTime; // 실행 시간을 밀리초 단위로 반환
    }
}

async function main() {
    console.log("Initializing MySQL...");
    const mysql = new Mysql("test", "test", "test");
    await mysql.init();
    console.log("MySQL initialized.");

    console.log("Initializing Neo4j...");
    const neo4j = new Neo4j("neo4j", "test", "test");
    await neo4j.init();
    console.log("Neo4j initialized.");

    console.log("Initializing ArangoDB...");
    const arango = new Arango("root", "test", "test");
    await arango.init();
    console.log("ArangoDB initialized.");

    console.log("Deleting all data from MySQL, Neo4j, and ArangoDB...");
    await mysql.deleteAll();
    await neo4j.deleteAll();
    await arango.deleteAll();
    console.log("All data deleted.");

    console.log("Creating tree...");
    const tree = createTree(treeDepth); // Create tree here
    console.log("Tree created.");

    console.log("Inserting data into MySQL...");
    await mysql.insertData(tree);
    console.log("Data inserted. MySQL");

    console.log("Inserting data into Neo4j...");
    await neo4j.insertData(tree);
    console.log("Data inserted. Neo4j");

    console.log("Inserting data into ArangoDB...");
    await arango.insertData(tree);
    console.log("Data inserted. ArangoDB");

    console.log("Executing Neo4j...");
    console.log(await neo4j.execute());
    console.log("Neo4j executed.");

    console.log("Executing MySQL...");
    console.log(await mysql.execute());
    console.log("MySQL executed.");

    console.log("Executing ArangoDB...");
    console.log(await arango.execute());
    console.log("ArangoDB executed.");
}

main();
