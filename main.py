import requests
from bs4 import BeautifulSoup
from dataclasses import dataclass
from typing import List, Dict
import json
import time
import random
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import pandas as pd

@dataclass
class Comment:
    id: int
    likes: int
    dislikes: int
    user_vote: int
    status: str
    ban: bool
    date_added: datetime
    url: str

def get_user_comments(session: requests.Session, account_id: int) -> Dict[str, List[Comment]]:
    """
    Получает все комментарии пользователя по его ID и разделяет их на группы
    """
    base_url = f"https://api.t-j.ru/ipa-gateway/api/v1/profiles/{account_id}/comments/"
    comments = {
        'only_likes': [],
        'only_dislikes': [],
        'both': []
    }
    offset = 0
    limit = 100
    total_comments = None
    processed_comments = 0
    
    # Вычисляем дату год назад
    one_year_ago = datetime.now(timezone.utc) - timedelta(days=365)
    
    while True:
        try:
            params = {
                'unsafe': 'true',
                'limit': limit,
                'offset': offset
            }
            
            response = session.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if total_comments is None:
                total_comments = data.get('count', 0)
            
            if not data.get('data'):
                break
                
            processed_comments += len(data['data'])
            
            for comment in data['data']:
                rating = comment.get('rating', {})
                likes = rating.get('likes', 0)
                dislikes = rating.get('dislikes', 0)
                
                # Пропускаем комментарии без лайков и дизлайков
                # или с суммой лайков и дизлайков меньше 5
                if likes + dislikes < 5:
                    continue
                
                # Проверяем дату комментария
                try:
                    date_added = datetime.fromisoformat(comment['date_added'].replace('Z', '+00:00'))
                    if date_added < one_year_ago:
                        break
                except (ValueError, KeyError):
                    continue
                    
                comment_obj = Comment(
                    id=comment['id'],
                    likes=likes,
                    dislikes=dislikes,
                    user_vote=rating.get('user_vote', 0),
                    status=comment['status'],
                    ban=comment.get('ban'),
                    date_added=date_added,
                    url=f'https://t-j.ru/{comment["article_path"]}/#c{comment["id"]}'
                )
                
                # Распределяем комментарии по группам
                if likes > 0 and dislikes > 0:
                    comments['both'].append(comment_obj)
                elif likes > 0:
                    comments['only_likes'].append(comment_obj)
                elif dislikes > 0:
                    comments['only_dislikes'].append(comment_obj)
            
            total_processed = len(comments['only_likes']) + len(comments['only_dislikes']) + len(comments['both'])
            progress = (processed_comments / total_comments) * 100 if total_comments > 0 else 0
            print(f"\rПолучено: {processed_comments}/{total_comments} ({progress:.1f}%), Учтено: {total_processed}", end='')
            
            if len(data['data']) < limit:
                break
                
            offset += limit
            time.sleep(0.5)  # Пауза 500 мс между запросами
            
        except requests.RequestException as e:
            print(f"\nОшибка при получении комментариев: {e}")
            break
        except Exception as e:
            print(f"\nНеожиданная ошибка: {e}")
            break
    
    print()  # Новая строка после прогресс-бара
    return comments

def process_user(session: requests.Session, user_id: int, progress_lock: Lock) -> Dict[str, List[Comment]]:
    """
    Обрабатывает одного пользователя
    """
    try:
        with progress_lock:
            print(f"\nОбработка пользователя ID: {user_id}")
        return get_user_comments(session, user_id)
    except Exception as e:
        with progress_lock:
            print(f"Ошибка при обработке пользователя {user_id}: {e}")
        return {'only_likes': [], 'only_dislikes': [], 'both': []}

def merge_comments(comments1: Dict[str, List[Comment]], comments2: Dict[str, List[Comment]]) -> Dict[str, List[Comment]]:
    """
    Объединяет результаты двух словарей с комментариями
    """
    result = {
        'only_likes': comments1['only_likes'] + comments2['only_likes'],
        'only_dislikes': comments1['only_dislikes'] + comments2['only_dislikes'],
        'both': comments1['both'] + comments2['both']
    }
    return result

def parse_tj_site():
    """
    Функция для парсинга сайта t-j.ru
    """
    session = requests.Session()
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0'
    }
    
    session.headers.update(headers)
    
    try:
        response = session.get('https://t-j.ru')
        response.raise_for_status()
        print("Куки получены успешно")
        print(f"Полученные куки: {dict(session.cookies)}")
        
        # Инициализируем общий результат
        all_comments = {
            'only_likes': [],
            'only_dislikes': [],
            'both': []
        }
        
        # Создаем блокировку для синхронизации вывода
        progress_lock = Lock()
        
        # Создаем пул потоков
        with ThreadPoolExecutor(max_workers=2) as executor:
            batch_size = 100  # Размер пакета пользователей для обработки
            processed_users = 0
            
            while True:
                # Проверяем, достигнуто ли минимальное количество комментариев
                if (len(all_comments['only_likes']) >= 2000 and 
                    len(all_comments['only_dislikes']) >= 2000 and 
                    len(all_comments['both']) >= 2000):
                    break
                
                # Генерируем новый пакет ID пользователей
                user_ids = random.sample(range(1, 10000000), batch_size)
                processed_users += batch_size
                
                # Запускаем обработку пользователей
                future_to_user = {
                    executor.submit(process_user, session, user_id, progress_lock): user_id 
                    for user_id in user_ids
                }
                
                # Обрабатываем результаты по мере их завершения
                for future in as_completed(future_to_user):
                    user_id = future_to_user[future]
                    try:
                        user_comments = future.result()
                        all_comments = merge_comments(all_comments, user_comments)
                        
                        # Выводим статистику
                        with progress_lock:
                            print(f"\nОбработано пользователей: {processed_users}")
                            print(f"Только лайки: {len(all_comments['only_likes'])}/2000")
                            print(f"Только дизлайки: {len(all_comments['only_dislikes'])}/2000")
                            print(f"И лайки, и дизлайки: {len(all_comments['both'])}/2000")
                        
                    except Exception as e:
                        with progress_lock:
                            print(f"Ошибка при обработке результатов пользователя {user_id}: {e}")
        
        # Выводим финальную статистику
        print("\nФинальная статистика:")
        for group_name, group_comments in all_comments.items():
            if group_comments:
                total_likes = sum(comment.likes for comment in group_comments)
                total_dislikes = sum(comment.dislikes for comment in group_comments)
                count = len(group_comments)
                
                avg_likes = total_likes / count
                avg_dislikes = total_dislikes / count
                
                print(f"\n{group_name.upper()}:")
                print(f"Количество комментариев: {count}")
                print(f"Среднее количество лайков: {avg_likes:.2f}")
                print(f"Среднее количество дизлайков: {avg_dislikes:.2f}")
                print(f"Общее количество лайков: {total_likes}")
                print(f"Общее количество дизлайков: {total_dislikes}")
        
        # Сохраняем все комментарии в CSV
        print("\nСохранение комментариев в CSV...")
        
        # Создаем список всех комментариев
        all_comments_list = []
        for group_name, comments in all_comments.items():
            for comment in comments:
                all_comments_list.append({
                    'id': comment.id,
                    'group': group_name,
                    'likes': comment.likes,
                    'dislikes': comment.dislikes,
                    'user_vote': comment.user_vote,
                    'status': comment.status,
                    'ban': comment.ban,
                    'date_added': comment.date_added,
                    'url': comment.url
                })
        
        # Создаем DataFrame и сохраняем в CSV
        df = pd.DataFrame(all_comments_list)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'comments_{timestamp}.csv'
        df.to_csv(filename, index=False, encoding='utf-8')
        print(f"Комментарии сохранены в файл: {filename}")
            
    except requests.RequestException as e:
        print(f"Ошибка при получении куки: {e}")

def main():
    parse_tj_site()

if __name__ == '__main__':
    main()
