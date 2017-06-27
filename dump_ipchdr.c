#include <stdio.h>
#include <sys/types.h>
#include <sys/msg.h>
#include <sys/shm.h>
#include <sys/sem.h>

#if 0
#define key  __key
#define seq  __seq
#endif
#define OFFSETOF(t, f) ((u_long)&(((t*)0)->f))
#define SIZEOF(t, f)   (sizeof(((t*)0)->f))

static FILE *fp = NULL;

#define PRINT_MACRO(name) \
	fprintf(fp, "  '%s': 0x%04x,\n", #name, name)
#define PRINT_STRUCT(t) \
	fprintf(fp, "  '%s': {\n    'size': %d,\n    'fields': [\n", #t, sizeof(t))
#define PRINT_FIELD(t, f) \
	fprintf(fp, "      ('%s', %d, %d),\n", #f, OFFSETOF(t,f), SIZEOF(t,f));
#define PRINT_STRUCT_END() \
	fprintf(fp, "    ]},\n")

int main()
{
	typedef struct ipc_perm ipc_perm;
	typedef struct msqid_ds msqid_ds;
	typedef struct shmid_ds shmid_ds;
	typedef struct semid_ds semid_ds;
	
	if (!(fp = fopen("./ipchdr.py", "w")))
	{
		perror("Failed to create ipchdr.py");
		return -1;
	}
	
	fprintf(fp, "ipc_define = {\n");
	PRINT_MACRO(IPC_PRIVATE);
	PRINT_MACRO(IPC_CREAT);
	PRINT_MACRO(IPC_EXCL);
	PRINT_MACRO(IPC_NOWAIT);
	PRINT_MACRO(SHM_RDONLY);
	PRINT_MACRO(SEM_UNDO);
	PRINT_MACRO(IPC_RMID);
	PRINT_MACRO(IPC_STAT);
	PRINT_MACRO(GETVAL);
	PRINT_MACRO(GETALL);
	PRINT_MACRO(SETVAL);
	PRINT_MACRO(SETALL);
	fprintf(fp, "}\n\n");
	
	fprintf(fp, "ipc_struct = {\n");
	
	PRINT_STRUCT(ipc_perm);
	PRINT_FIELD(ipc_perm, key);
	PRINT_FIELD(ipc_perm, uid);
	PRINT_FIELD(ipc_perm, gid);
	PRINT_FIELD(ipc_perm, cuid);
	PRINT_FIELD(ipc_perm, cgid);
	PRINT_FIELD(ipc_perm, mode);
	PRINT_FIELD(ipc_perm, seq);
	PRINT_STRUCT_END();
	
	PRINT_STRUCT(msqid_ds);
	PRINT_FIELD(msqid_ds, msg_stime);
	PRINT_FIELD(msqid_ds, msg_rtime);
	PRINT_FIELD(msqid_ds, msg_ctime);
	PRINT_FIELD(msqid_ds, msg_cbytes);
	PRINT_FIELD(msqid_ds, msg_qnum);
	PRINT_FIELD(msqid_ds, msg_qbytes);
	PRINT_FIELD(msqid_ds, msg_lspid);
	PRINT_FIELD(msqid_ds, msg_lrpid);
	PRINT_STRUCT_END();

	PRINT_STRUCT(shmid_ds);
	PRINT_FIELD(shmid_ds, shm_segsz);
	PRINT_FIELD(shmid_ds, shm_atime);
	PRINT_FIELD(shmid_ds, shm_dtime);
	PRINT_FIELD(shmid_ds, shm_ctime);
	PRINT_FIELD(shmid_ds, shm_cpid);
	PRINT_FIELD(shmid_ds, shm_lpid);
	PRINT_FIELD(shmid_ds, shm_nattch);
	PRINT_STRUCT_END();

	PRINT_STRUCT(semid_ds);
	PRINT_FIELD(semid_ds, sem_otime);
	PRINT_FIELD(semid_ds, sem_ctime);
	PRINT_FIELD(semid_ds, sem_nsems);
	PRINT_STRUCT_END();

	fprintf(fp, "}\n");
	fclose(fp);
	return 0;
}
