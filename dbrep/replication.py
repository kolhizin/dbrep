from cmath import log
import logging

logger = logging.getLogger(__name__)

def full_refresh(srcEngine, dstEngine, repConfig):
    raise NotImplemented

def incremental_update(srcEngine, dstEngine, repConfig):
    logger.debug('Making request to get <src> latest rid...')
    src_rid = srcEngine.get_latest_rid(repConfig['src'])

    logger.debug('Making request to get <dst> latest rid...')
    dst_rid = dstEngine.get_latest_rid(repConfig['dst'])

    logger.info('Latest rids: <src>={}, <dst>={}'.format(src_rid, dst_rid))

    src_batch_size = repConfig['src'].get('batch_size', 1000)
    dst_batch_size = repConfig['dst'].get('batch_size', 1000)
    while dst_rid < src_rid:
        logger.info('Starting src-request')
        srcEngine.start_get_inc(repConfig['src'], dst_rid)

        while True:
            logger.debug('Fetching src-batch')
            src_batch = srcEngine.get_batch(src_batch_size)
            if src_batch is None or len(src_batch) == 0:
                break

            for off in range(0, len(src_batch), dst_batch_size):
                names, dst_batch = src_batch[off:(off + dst_batch_size)]
                logger.debug('Saving dst-batch')
                dstEngine.insert_batch(repConfig['dst'], dst_batch, names)
            logger.debug('Saved src-batch')
        
        logger.info('Finished sync. Updating <dst> rid...')
        dst_rid = dstEngine.get_latest_rid(repConfig['dst'])
        logger.info('Latest rids: <src>={} (old), <dst>={} (updated)'.format(src_rid, dst_rid))
    logger.info('Replication finished.')