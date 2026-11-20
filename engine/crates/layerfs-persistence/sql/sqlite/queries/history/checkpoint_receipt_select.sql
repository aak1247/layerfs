SELECT request_digest,branch_id,base_layer_id,outcome,result_commit_id,root_id,displaced_id,lww_truncated FROM checkpoint_receipt WHERE attempt_id=$1;
