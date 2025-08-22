#!/bin/bash
echo 'Waiting for MongoDB to be ready...'

sleep 5
until mongosh --quiet -u ${MONGO_INITDB_ROOT_USERNAME} -p ${MONGO_INITDB_ROOT_PASSWORD} --host ${MONGO_HOST} --eval 'db.adminCommand("ping")'; do
    echo 'MongoDB is not ready yet, retrying...'
    sleep 2
done

sleep 2
echo 'MongoDB ready, checking replicaSet status...';
mongosh --quiet -u ${MONGO_INITDB_ROOT_USERNAME} -p ${MONGO_INITDB_ROOT_PASSWORD} --host ${MONGO_HOST} --eval '
    const replicaSet = process.env.MONGO_REPLICA_SET_NAME;
    try {
        rs.status();
    } catch (e) {
        print("ReplicaSet status error: " + e);
        print("Initializing ReplicaSet...");
        try {
            rs.initiate({
                _id: replicaSet,
                members: [
                    {_id: 0, host: "mongo"},
                    {_id: 1, host: "mongo-rep1"},
                    {_id: 2, host: "mongo-rep2"}
                ]
            });
            print("ReplicaSet initialized successfully");
        } catch (e) {
            print("Error initializing ReplicaSet: " + e);
        }
    }
';

sleep 5
echo 'Waiting for primary...'
until mongosh --quiet -u ${MONGO_INITDB_ROOT_USERNAME} -p ${MONGO_INITDB_ROOT_PASSWORD} --host ${MONGO_HOST} --eval 'rs.isMaster().ismaster' | grep -q true; do
    echo 'Primary not elected yet, retrying...'
    sleep 2
done

echo 'Primary elected, creating admin user...'
mongosh -u ${MONGO_INITDB_ROOT_USERNAME} -p ${MONGO_INITDB_ROOT_PASSWORD} --host ${MONGO_HOST} --eval '
    const adminUser = process.env.MONGO_ADMIN_USER;
    db = db.getSiblingDB("admin");
    try {
        if (!db.getUser(process.env.MONGO_ADMIN_USER)) {
            print("Creating admin user: " + adminUser);
            db.createUser({user: process.env.MONGO_ADMIN_USER, pwd: process.env.MONGO_ADMIN_PASS, roles: [ { role: "root", db: "admin" } ]});
            print("Admin user created successfully");
        }
    } catch (e) {
        print("Error creating admin user: " + e);
    }

    print("Creating app database & admin user...");
    appDb = process.env.MONGO_APP_DB;
    try {
        db = db.getSiblingDB(appDb);
        db.createUser({
            user: process.env.MONGO_APP_USER,
            pwd: process.env.MONGO_APP_PASS,
            roles: [{ role: "readWrite", db: appDb }]
        });
    } catch (e) {
        print("Error creating app admin user & db: " + e);
    }

    print("Creating test database & test user...");
    try {
        db = db.getSiblingDB(appDb);
        db.createUser({
            user: "testuser",
            pwd: "testpassword",
            roles: [{ role: "readWrite", db: `${appDb}_test` }]
        });
    } catch (e) {
        print("Error creating test user & db: " + e);
    }
';
echo 'MongoDB setup completed.'
exit 0
