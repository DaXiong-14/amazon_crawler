import logging

from src.amazon_selection_crawler import updataItems
from tool.keywords_amount_utils import export_token
from tool.utils import _read_user

logger = logging.getLogger(__name__)

def master(site, items):
    """
    :param site:
    :param items:
    :return:
    """
    # todo 5. 获取 token
    user = _read_user()
    token = export_token(user.get('username'), user.get('password'))

    # todo 6. 取 asins
    asinList = []
    for item in items:
        asinList.append(item.get('asin'))
    conf = {'site':site}
    newItems = updataItems(items, asinList, token, conf, t=False)
    results = []
    for item in newItems:
        try:
            days = item.get('available_days')
            if int(days) < 183:
                results.append(item)
        except Exception as e:
            logger.error(f'日期错误 {e}')

    return results



