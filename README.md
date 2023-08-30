# cmdb

Work in progress.

## Introduction

This is a graph based Configuration Management DataBase or CMDB. The core 
components of this tool are the Configuration Item (CI) and Relationship. By
creating and linking CIs together with Relationships, you can model the real
world relationships between things, find faults and discover dependencies.

Currently, the API is not written and only a basic example is provided.

## Internals

The CMDB is based on the bbolt KV database. IDs are generated as ULIDs.
