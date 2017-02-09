Should physical table continue to be mutable? 

If NOT how to we initialize it?  
When do we change the object reference in the table dictionary?
When do we change the object references in the logical tables?

If SO, who or what do we notify in the event of changes?
e.g. If a composite physical table references physical tables which change
under it, how does it know so that it can correctly update the world?

Which parts of a physical table can change?

- Available intervals

- Columns (? config time only, perhaps?)

- when Dimensions change, does the physical table get notified?
