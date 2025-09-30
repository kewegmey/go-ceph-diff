# rbd-diff-apply

Tool to apply ceph rbd diffs to a block device.

Idea is to run rbd export @snap1 piped to a ZFS zvol for the inital full backup.  Then, to do incrementals run rbd export-diff piped through this tool to apply the diff to the zvol.  
