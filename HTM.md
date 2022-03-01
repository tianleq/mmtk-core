### BLOCKING ISSUES IN OPENJDK

- Handshake between Mutator threads and Collector threads
One critical step of the algorithm is to make sure all mutator threads acknowledge the latest copying state. To achieve this, collectors will do handshake with all mutator threads(spinning until all mutator threads see the latest value). Collectos might spin forever if some mutator threads has already been blocked before the copying state is updated. As for threads spawned after the handshake, they are not handled properly as well.

- Safepoint mechanism
In order to make sure mutator threads and collector threads do not conflict with each other(write to the object while it is being copied), mutator threads have to execute in a transactions between two consecutive safepoints. Given that OpenJDK is using memory protection to trigger safepoint, transaction will always abort due to trapping into the kernel. As a result, mutator threads literally cannot make any progress and thus it nullify "concurrent".

- Off-heap barrier
Give that the write capacity is quite limited within a transaction(roughly 80% of L1 cache), the algorithm uses write barrier to capture all updates to roots and put them in a remember set. The remember set will be processed in the transaction to make sure roots are pointing to new object references. OpenJDK does not ahave barrier that can capture all write to off-heap roots. To ensure the correctness of the algorithm, a root trace needs to be done in the transaction and it is highly likely to abort the transaction due to exceeding the transaction capacity
 
