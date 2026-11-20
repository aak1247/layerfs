INSERT INTO checkpoint_receipt (attempt_id,request_digest,branch_id,base_layer_id,outcome,result_commit_id,root_id,displaced_id,lww_truncated) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING 1;
