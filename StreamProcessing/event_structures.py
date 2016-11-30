# -*- coding: utf-8 -*-
class Post:
    """
    Representa um evento do tipo "Post".
    """
    def __init__(self, event):
        """
        Inicializa o objeto a partir de um string no formato de um evento
        'post'.

        formato do string:
        <ts>|<post_id>|<user_id>|<content>|<user_name>

        ts (string):        timestamp no formato 'yyyy-mm-ddTHH:mm:ss.mmm+TzTz'
        post_id (integer):  ID do post, tipo inteiro.
        user_id (integer):  ID do usuário criador do post.
        content (string):   Mensagem do post.
        user_name (string): Nome do usuário criador do post.
        :param event: String no formato de um evento do tipo 'post'
        """
        segments = event.split('|')

        self.timestamp = segments[0]
        self.id = segments[1]
        self.user_id = segments[2]
        self.message = segments[3]
        self.user_name = segments[4]

        self.score = 10
        self.total_score = 10

        self.comments_num = 0
        self.comments = []

    def __increment_total_score__(self):
        self.total_score += 10

    def decrement_total_score(self):
        if self.total_score > 0:
            self.total_score -= 1

    def decrement_self_score(self):
        if self.score > 0:
            self.score -= 1
            self.decrement_total_score()

    def is_active(self):
        """
        Verifica se o post se encontra ativo, um post é considerado ativo
        enquanto tiver uma pontuação maior do que zero. Note que a pontuação
        própria do post é independente da pontuação TOTAL da estrutura do post.

        :return: True se estiver ativo, False caso contrário.
        """
        return self.score > 0

    def insert_comment(self, comment):
        """
        Insere o comentário a sua lista de comentários relacionados e atualiza
        a pontuação total do post.

        :param comment: Comentário relacionado direta ou indiretamente ao post.
        :return: None
        """
        comment.set_parent_post(self)
        self.comments.append(comment)
        self.__increment_total_score__()

    def is_parent_of(self, comment):
        print("\nPOST ID: %s" % self.id)
        print("COMMENT: %s" % comment.id)
        if self.id == comment.commented_post_id:
            return True

        if comment.commented_post_id != '-1':
            return False

        for element in self.comments:
            print("COMMENT.ID FROM POST'S LIST: %s" % element.id)
            if comment.replied_comment_id == element:
                return True

        return False


# TODO: Query 2
class Friendship:
    # is the friendship’s establishment timestamp
    timestamp = None

    # is the id of one of the users
    user_id_1 = None

    # is the id of the other user
    user_id_2 = None

    def __init__(self, event):
        segments = event.split('|')

        self.timestamp = segments[0]
        self.user_id_1 = segments[1]
        self.user_id_2 = segments[2]


class Comment:
    """
    Representa um evento do tipo 'Comentário'
    """
    def __init__(self, event):
        segments = event.split('|')

        self.timestamp = segments[0]
        self.id = segments[1]
        self.user_id = segments[2]
        self.message = segments[3]
        self.user_name = segments[4]
        self.replied_comment_id = segments[5]
        self.commented_post_id = segments[6]

        self.parent_post = None
        self.score = 10

    def decrement_score(self):
        """
        Decrementa tanto sua pŕopria pontuação quanto a pontuação total do post
        a que se relaciona.

        :return: None
        """
        if self.score > 0:
            self.score -= 1
            self.parent_post.decrement_total_score()

    def set_parent_post(self, post):
        """
        Estabelece o post ao qual o comentário se relaciona, diretamente ou
        indiretamente.

        :param post: Post relacionado
        :return: None
        """
        self.parent_post = post

    def is_active(self):
        """
        Verifica se o comentário se encontra ativo, um comentário é considerado
        ativo enquanto tiver uma pontuação maior do que zero.

        :return: True se estiver ativo, False caso contrário.
        """
        return self.score > 0


# TODO: Query 2
class Like:
    # is the like’s timestamp
    timestamp = None

    # is the id of the user liking the comment
    user_id = None

    # is the id of the comment
    comment_id = None

    def __init__(self, event):
        segments = event.split('|')

        self.timestamp = segments[0]
        self.user_id = segments[1]
        self.comment_id = segments[2]
