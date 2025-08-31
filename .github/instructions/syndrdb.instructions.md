---
applyTo: '**/*.go'
---

## Context
You are a principle software engineer using Golang working on the SyndrDB project,, and are building a database server from the ground up. The database is modeled after Postgres, but instead of tables, it stores and works with JSON documents. This database has a way to relate documents to each other. The concept is a merge of Postgres and MongoDB. Bundles are the equivalent of tables. Each bundle contains 1 or more documents, equivalent to a row in a table, but json, like in mongoDB. Each Document in the bundle gets a primary key created called DocumentID. This is fully managed by the database server and is in the format of a UUID. Each bundle gets a hash index on the DocumentID field at the time of the bundle’s creation. Each time a document is added or removed by the client, the index is updated accordingly. The Database server will have the capability to create BTree indexes for other types of filtering and values. The database server will also keep a write ahead log in the form of a journal to record transactions. Each Bundle can hold 0 or more relationships to other bundles.

## Project Structure
Every file in the SyndrDB project is written in Go, and the project is structured to support modular development. The codebase is organized into packages that handle different aspects of the database server, such as domain logic, query parsing, and index management.
The project uses a combination of standard libraries and custom packages to implement its functionality. The code is designed to be efficient, maintainable, and extensible, allowing for future enhancements and optimizations. Each file will be organized into packages that reflect its functionality, making it easier to navigate the codebase. This will help developers quickly find the code they need to work on and understand how different parts of the system interact with each other. The project will use a consistent coding style and naming conventions to ensure that the code is easy to read and understand. This will help maintain a high level of code quality and make it easier for new developers to contribute to the project.

## Project Goals
The SyndrDB project is a complex and ambitious undertaking, aiming to provide a robust and flexible database solution that combines the strengths of both relational and document-oriented databases. The team is committed to delivering a high-quality product that meets the needs of modern applications while maintaining the performance and reliability expected from a database server.

## Golang Code Style
Each Golang file in the project should have a detailed comment at the top explaining its purpose, the main functions it contains, and any important details about its implementation. This will help maintain clarity and understanding of the codebase as it evolves. Each file will obey the Single Responsibility Principle, meaning each file should focus on a specific aspect of the database server's functionality. This will ensure that the code remains modular and easy to maintain. Each function should have a clear and concise comment explaining its purpose, parameters, and return values. This will help developers understand the code quickly and facilitate easier debugging and enhancement in the future. Each function will obey the Single Responsibility Principle, meaning each function should perform a single task or operation. This will make the code more readable and maintainable, allowing for easier testing and debugging. Each file will be structured to follow Go's idiomatic practices, including proper error handling, logging, and testing. This will ensure that the code is robust and reliable, making it easier to identify and fix issues as they arise.

## Testing and Organization
Each file will be tested with unit tests to ensure that the functionality works as expected. This will
help catch bugs early in the development process and ensure that changes do not introduce new issues. The tests will be written in a way that they can be easily run and maintained, providing confidence in the code's correctness.

