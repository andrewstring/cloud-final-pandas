def create_users_file():
    open("./users.txt", "w")

def user_add(username, password, email):
    username_list = []
    with open("./users/users.txt", "r") as users:
        individual_users = users.readlines()
        for user in individual_users:
            username_list.append(user.split(",")[0])

    if username not in username_list:
        with open("./users/users.txt", "a") as users:
            users.write(",".join([username, password, email])+"\n")
            return 1
    else:
        return 0

def user_login(username, password):
    username_list = []
    with open("./users/users.txt", "r") as users:
        individual_users = users.readlines()
        for user in individual_users:
            username_list.append(user.split(",")[0:2])

    if username not in [user[0] for user in username_list]:
        return 0
    if username_list[[user[0] for user in username_list].index(username)][1] != password:
        return 1
    else:
        return 2
        


if __name__ == "__main__":
    create_users_file()