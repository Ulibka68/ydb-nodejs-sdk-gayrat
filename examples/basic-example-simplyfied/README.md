1. Создай каталог test2 через web интерфейс, запиши его id в deploy/locals.tf
2. Создай в каталоге test2 сервисный аккаунт test2-admin с ролью admin

3. Требуется создать новый профиль yc для работы с каталогом
  Выполните команду:

```bash
yc config profile list
yc config profile activate default
yc config profile delete ssl-webapp

yc config profile create test2
yc config set cloud-id b1gib03pgvqrrfvhl3kb
yc config set folder-id b1gku2m6mn7tb2d3ib91 
yc config set token Agююююююююююююююююююююююююts
yc config profile get test2
yc config profile activate test2
```

2.0. Создать токен
[Получи токен и добавь его к YC](https://cloud.yandex.ru/docs/cli/quickstart)

yc config list

2.2.
[Сформировать файл ключей DOC](https://cloud.yandex.com/en-ru/docs/iam/operations/iam-token/create-for-sa#via-cli)
Файл ключей формируется для сервисного аккаунт с ролью admin
```bash
yc iam key create --service-account-name test2-admin --output service_account_key_file.json
```

2.3. Назначить роль на сервисный аккаунт
[Назначить роль на сервисный аккаунт](https://cloud.yandex.ru/docs/iam/operations/sa/set-access-bindings#assign-role-to-sa)

```bash
yc iam role list
yc iam service-account  get test2
yc iam service-account add-access-binding 
```
