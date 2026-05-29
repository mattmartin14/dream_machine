### Primary Key Management

In data warehousing, we have 2 traditional ways of modeling tables with primary keys:

1. Surrogate (1-up) key
2. Natural Key

### Pros and Cons
For surrogate keys, here are the pros/cons:
- pro: very fast joins; since its a single column and highly indexible, it can sort/join usually significantly faster than a natural key
- pro: keeps the join data types uniform; almost all surrogate keys are ints or bigints, depending on if the dataset has to warehouse billions of rows
- con: recasting data can be complex; you have to reseed the 1-up key and ensure the various tables that use it are in sync
- con: migrating from one dw to another can be challenging when migrating the surrogate keys over and having it resume its seed at a specific value

Natural Keys Pros/Cons:
- Pro: usually more human readable and understandable
- Pro: Recasts are very easy; no need to resync 1-up keys
- Con: joins can sometimes be much slower if the key field is a large data type or something that doesnt sort that well such as a GUID
- Con: natural keys can sometimes be several columns (composite keys); the join predicates can become very lengthy

