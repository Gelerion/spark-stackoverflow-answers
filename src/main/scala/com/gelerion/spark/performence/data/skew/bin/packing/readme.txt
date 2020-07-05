1. Reviewing the Ganglia metrics for my cluster
   - Memory usage
     Memory usage ramps up quickly at the start of the job run and then reduces in steps over time.
     As nodes in the cluster finish processing they release their memory and that is reflected in the trace.
     We can see that one node seems to be processing data long after all the other nodes have finished processing.
   - Load traces for the individual nodes

Algorithm:
 Term Frequency - Inverse Document Frequency

 W(term, doc) = TF(ter, doc) * log(N/DF(term))
 W(term, doc)  - weight of term in doc
 TF(ter, doc) - frequency of term in doc
 DF(term) - number of documents containing term
 N - total number of documents


 So what can we do about data skew? When we look at data by partition we can identify heavily weighted
 partitions and that a number of the larger bookshelves are clumped together. We know that Spark assigns
 one task per partition and each worker can process one task at a time. This means the more partitions
 the greater potential parallelism, given enough hardware to support it.