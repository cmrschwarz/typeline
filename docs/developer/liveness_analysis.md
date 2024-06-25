# Liveness Analysis Explanation

## Data Structure

### Every BB is initialized with the following static data:
 - *Predecessors*: list of all BBs that may preceed this block (including callers)
 - *Callees*: list of BBs that this block may call (call stack semantics). It is assumed that exactly one callee is called each time (unless there are none).
 - *Successors*: list of all BBs that may succeed this block (after a potential call returned). Like with gen/kill, it is assumed that exactly one of these is used each time (unless there are none).
 - *Local Accesses*: Set of variables accessed by that are accessed (read) by this block before any reassignments.
 - *Local Survives*: Set of variables that are **not** reassigned by this block.

### Each BB has two additional properties that will be dynamically updated:

 - *Global Accesses*: Set of variables accessed (before any reassignment) by this block directly or though any of it's continuations. This will be the final Liveness output. Initially, this set contains **all** variables.

 - *Global Survives*: Set of all variables that are **not** definitely (in all possible executions) reassigned by this block or it's continuations.
Initially, this set contains **all** variables.

## Algorithm

 
 1. Put all BBs into a Work Queue **Q**.
 2. Take out one BB (**B**) from **Q** .
 3. For each variable **V** in **B**'s *Global Accesses* set, remove it if **all** of the following 3 conditions are true:
    - **V** is not part of **B**'s *Local Accesses*
    - **V** is not in the *Global Accesses* of any of **B**'s *Callees*
    - **Any** of the two following conditions is true:
        - **V** is not in the *Global Survives* of any of **B**'s *Callees*  (not applicable if there are none)
        - **V** is not in the *Global Accesses* of any of **B**'s *Successors*   

 4. For each variable **V** in **B**'s *Global Survives* set, remove it if **any** of the following 3 conditions are  true:
    - **V** is not part of **B**'s *Local Survives*
    - **V** is not in the *Global Survives* of any of **B**'s *Callees* (not applicable if there are none)
    - **V** is not in the *Global Survives* of any of **B**'s *Successors* (not applicable if there are none)
 5. If steps 3 or 4 changed the *Global Survives* or *Global Accesses* of
    **B** in any way, enqueue all of **B**s *Predecessors* into **Q**, if they aren't already in it.
 6. If **Q** is not empty, repeat from step 2.


## Example

```js
var A;
var B;
var C;

function main() {        // BB 1
    bar();               
    if A {               // BB 2
        baz();
    }
    else {               // BB 3 (needed to encode 2 being an optional successor of 1)

    }
}

function bar() {         // BB 4
    B = C;
    C = A;
}

function baz() {         // BB 5
    A = B;
    bar();
}

```

### Initial Data
| **BB** | *Predecessors* | *Callees* | *Successors* | *Local Accesses* | *Local Survives* | *Global Accesses* | *Global Survives* |
|--------|----------------|-----------|--------------|------------------|------------------|-------------------|-------------------|
| **1**  | []             | [4]       | [2, 3]       | {A}              | {A,B,C}          | {A,B,C}           | {A,B,C}           |
| **2**  | [1]            | [5]       | []           | {}               | {A,B,C}          | {A,B,C}           | {A,B,C}           |
| **3**  | [1]            | []        | []           | {}               | {A,B,C}          | {A,B,C}           | {A,B,C}           |
| **4**  | [1,5]          | []        | []           | {A,C}            | {A}              | {A,B,C}           | {A,B,C}           |
| **5**  | [2]            | [4]       | []           | {B}              | {B,C}            | {A,B,C}           | {A,B,C}           |

### TODO: finish this
### Algorithm Execution
| **BB Selected** | *New Global Accesses* | *New Global Survives* | *Remaining Queue* |
|-----------------|-----------------------|-----------------------|-------------------|
| **5**           | {A,B,C}               | {B,C}                 | [4,3,2,1]         |
| **4**           | {A,C}                 | {A}                   | [5,3,2,1]         |
