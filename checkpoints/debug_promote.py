from ffengine.ui.studio_service import promote_dag_revision
import traceback
try:
    result = promote_dag_revision(dag_id='webhook_whk_level1_src_to_stg_group_1_dag', revision_id='rev_000001')
    print('OK', result)
except Exception as e:
    traceback.print_exc()
    print('ERR_TYPE', type(e).__name__)
    print('ERR_MSG', str(e))
    if e.__cause__ is not None:
        print('CAUSE_TYPE', type(e.__cause__).__name__)
        print('CAUSE_MSG', str(e.__cause__))
