#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define GRUPO "225.0.0.37"
#define PORTA 12345
#define MAX_PARTICIPANTES 64
#define TAM_NOME 32
#define TAM_ID 64
#define TAM_BUFFER 1024
#define TAM_LINHA 512

typedef struct {
    char id[TAM_ID];
    int ativo;
} Participante;

/* a tabela é usada pela thread de recepção e pela principal, então precisa do mutex */
Participante tabela[MAX_PARTICIPANTES];
pthread_mutex_t mutex_tabela = PTHREAD_MUTEX_INITIALIZER;

int soquete;
struct sockaddr_in endereco_grupo;
char meu_id[TAM_ID];
volatile sig_atomic_t executando = 1;

void adicionar_participante(const char *id) {
    int i, vaga = -1;

    pthread_mutex_lock(&mutex_tabela);
    for (i = 0; i < MAX_PARTICIPANTES; i++) {
        if (tabela[i].ativo && strcmp(tabela[i].id, id) == 0) {
            pthread_mutex_unlock(&mutex_tabela);
            return;
        }
        if (!tabela[i].ativo && vaga == -1)
            vaga = i;
    }
    if (vaga != -1) {
        snprintf(tabela[vaga].id, TAM_ID, "%s", id);
        tabela[vaga].ativo = 1;
    }
    pthread_mutex_unlock(&mutex_tabela);
}

void remover_participante(const char *id) {
    int i;

    pthread_mutex_lock(&mutex_tabela);
    for (i = 0; i < MAX_PARTICIPANTES; i++) {
        if (tabela[i].ativo && strcmp(tabela[i].id, id) == 0) {
            tabela[i].ativo = 0;
            break;
        }
    }
    pthread_mutex_unlock(&mutex_tabela);
}

void listar_participantes() {
    int i, total = 1;

    pthread_mutex_lock(&mutex_tabela);
    printf("Quem está no chat:\n");
    printf("  %s (você)\n", meu_id);
    for (i = 0; i < MAX_PARTICIPANTES; i++) {
        if (tabela[i].ativo) {
            printf("  %s\n", tabela[i].id);
            total++;
        }
    }
    printf("%d no total\n", total);
    pthread_mutex_unlock(&mutex_tabela);
}

void enviar(const char *tipo, const char *conteudo) {
    char texto[TAM_BUFFER];
    int tamanho;

    tamanho = snprintf(texto, sizeof texto, "%s|%s|%s", tipo, meu_id, conteudo);
    if (tamanho < 0)
        return;
    if (tamanho >= (int)sizeof texto)
        tamanho = sizeof texto - 1;

    if (sendto(soquete, texto, tamanho, 0,
               (struct sockaddr *)&endereco_grupo, sizeof endereco_grupo) < 0)
        perror("sendto");
}

void tratar_mensagem(char *texto) {
    char *tipo = texto;
    char *id, *conteudo;

    id = strchr(texto, '|');
    if (id == NULL)
        return;
    *id = '\0';
    id++;

    conteudo = strchr(id, '|');
    if (conteudo == NULL)
        return;
    *conteudo = '\0';
    conteudo++;
    conteudo[strcspn(conteudo, "\r\n")] = '\0';

    if (id[0] == '\0' || strlen(id) >= TAM_ID)
        return;

    /* o loopback do multicast devolve o que a gente mesmo enviou, então ignora */
    if (strcmp(id, meu_id) == 0)
        return;

    if (strcmp(tipo, "ENTRADA") == 0) {
        adicionar_participante(id);
        printf("%s entrou no chat\n", id);
        enviar("PRESENCA", "");
    } else if (strcmp(tipo, "PRESENCA") == 0) {
        adicionar_participante(id);
    } else if (strcmp(tipo, "SAIDA") == 0) {
        remover_participante(id);
        printf("%s saiu do chat\n", id);
    } else if (strcmp(tipo, "MENSAGEM") == 0) {
        printf("[%s] %s\n", id, conteudo);
    }
    fflush(stdout);
}

void *receber(void *arg) {
    char texto[TAM_BUFFER];
    ssize_t recebidos;

    (void)arg;
    while (executando) {
        recebidos = recvfrom(soquete, texto, sizeof texto - 1, 0, NULL, NULL);
        if (recebidos < 0) {
            if (errno == EAGAIN || errno == EINTR)
                continue;
            if (!executando)
                break;
            perror("recvfrom");
            break;
        }
        texto[recebidos] = '\0';
        tratar_mensagem(texto);
    }
    return NULL;
}

void ao_interromper(int sinal) {
    (void)sinal;
    executando = 0;
}

int main(int argc, char *argv[]) {
    struct sockaddr_in endereco_local;
    struct ip_mreq requisicao_grupo;
    struct sigaction acao;
    struct timeval limite_tempo;
    pthread_t thread_recepcao;
    char entrada[TAM_LINHA];
    int valor, tamanho;

    if (argc != 2) {
        fprintf(stderr, "Uso: %s <nome>\n", argv[0]);
        return 1;
    }
    if (argv[1][0] == '\0' || strchr(argv[1], '|') || strchr(argv[1], '#')) {
        fprintf(stderr, "Nome inválido, não pode ser vazio nem ter '|' ou '#'\n");
        return 1;
    }

    tamanho = snprintf(meu_id, sizeof meu_id, "%.*s#%ld",
                       TAM_NOME - 1, argv[1], (long)getpid());
    if (tamanho < 0 || tamanho >= (int)sizeof meu_id) {
        fprintf(stderr, "Erro ao montar o ID\n");
        return 1;
    }

    memset(&acao, 0, sizeof acao);
    acao.sa_handler = ao_interromper;
    sigemptyset(&acao.sa_mask);
    sigaction(SIGINT, &acao, NULL);

    soquete = socket(AF_INET, SOCK_DGRAM, 0);
    if (soquete < 0) {
        perror("socket");
        return 1;
    }

    /* sem isso só um peer por máquina conseguiria usar a porta 12345 */
    valor = 1;
    if (setsockopt(soquete, SOL_SOCKET, SO_REUSEADDR, &valor, sizeof valor) < 0) {
        perror("SO_REUSEADDR");
        return 1;
    }
#ifdef SO_REUSEPORT
    if (setsockopt(soquete, SOL_SOCKET, SO_REUSEPORT, &valor, sizeof valor) < 0) {
        perror("SO_REUSEPORT");
        return 1;
    }
#endif

    memset(&endereco_local, 0, sizeof endereco_local);
    endereco_local.sin_family = AF_INET;
    endereco_local.sin_addr.s_addr = htonl(INADDR_ANY);
    endereco_local.sin_port = htons(PORTA);
    if (bind(soquete, (struct sockaddr *)&endereco_local, sizeof endereco_local) < 0) {
        perror("bind");
        return 1;
    }

    memset(&requisicao_grupo, 0, sizeof requisicao_grupo);
    requisicao_grupo.imr_multiaddr.s_addr = inet_addr(GRUPO);
    requisicao_grupo.imr_interface.s_addr = htonl(INADDR_ANY);
    if (setsockopt(soquete, IPPROTO_IP, IP_ADD_MEMBERSHIP,
                   &requisicao_grupo, sizeof requisicao_grupo) < 0) {
        perror("IP_ADD_MEMBERSHIP");
        return 1;
    }

    valor = 1;
    if (setsockopt(soquete, IPPROTO_IP, IP_MULTICAST_LOOP, &valor, sizeof valor) < 0) {
        perror("IP_MULTICAST_LOOP");
        return 1;
    }
    valor = 1;
    if (setsockopt(soquete, IPPROTO_IP, IP_MULTICAST_TTL, &valor, sizeof valor) < 0) {
        perror("IP_MULTICAST_TTL");
        return 1;
    }

    /* timeout de 1s no recvfrom, assim a thread consegue perceber que é hora de parar */
    limite_tempo.tv_sec = 1;
    limite_tempo.tv_usec = 0;
    if (setsockopt(soquete, SOL_SOCKET, SO_RCVTIMEO,
                   &limite_tempo, sizeof limite_tempo) < 0) {
        perror("SO_RCVTIMEO");
        return 1;
    }

    memset(&endereco_grupo, 0, sizeof endereco_grupo);
    endereco_grupo.sin_family = AF_INET;
    endereco_grupo.sin_addr.s_addr = inet_addr(GRUPO);
    endereco_grupo.sin_port = htons(PORTA);

    if (pthread_create(&thread_recepcao, NULL, receber, NULL) != 0) {
        fprintf(stderr, "Erro ao criar a thread de recepção\n");
        return 1;
    }

    printf("Você é %s. Comandos: /quem e /sair\n", meu_id);
    fflush(stdout);

    enviar("ENTRADA", "");

    while (executando && fgets(entrada, sizeof entrada, stdin) != NULL) {
        entrada[strcspn(entrada, "\r\n")] = '\0';
        if (entrada[0] == '\0')
            continue;

        if (strcmp(entrada, "/sair") == 0)
            break;
        else if (strcmp(entrada, "/quem") == 0)
            listar_participantes();
        else if (entrada[0] == '/')
            printf("Comando desconhecido, use /quem ou /sair\n");
        else
            enviar("MENSAGEM", entrada);
        fflush(stdout);
    }

    /* avisa a saída pro grupo antes de largar a associação multicast */
    enviar("SAIDA", "");
    if (setsockopt(soquete, IPPROTO_IP, IP_DROP_MEMBERSHIP,
                   &requisicao_grupo, sizeof requisicao_grupo) < 0)
        perror("IP_DROP_MEMBERSHIP");

    executando = 0;
    pthread_join(thread_recepcao, NULL);
    close(soquete);
    printf("Até logo!\n");
    return 0;
}