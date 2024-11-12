# user_prompt = "Enter a ToDo:"
# user_text1 = input(user_prompt)
# user_text2 = input(user_prompt)
# user_text3 = input(user_prompt)
#
# todo = [user_text1, user_text2, user_text3]
# print(todo)
#
# print(type(todo))

# ==================================================== #

# user_prompt = "Enter a Todo: "
# todos = []
# while True:
#     todo = input(user_prompt)
#     print(todo.capitalize())
#     todos.append(todo)

# ==================================================== #

# todos = []
#
# while True:
#     user_text = input("Type add, show or exit: ")
#     user_text = user_text.strip()
#
#     match user_text:
#         case 'add':
#             todo = input("Enter a todo:")
#             todos.append(todo)
#         case 'show':
#             for item in todos:
#                 item = item
#         case 'edit':
#             num = int(input("Number of todo to be edited:"))
#             num = num - 1
#             new_todo = input("Enter a new todo: ")
#             todos[num] == new_todo
#         case 'exit':
#             break
#
#     print('Thanks for visiting')
#
# for index, item in enumerate(todos):
#     row = f"{index + 1}-{item}"
#     print(row)
#
# # ==================================================== #
# if 'add' in user_text:
#     todo = user_text[4:]
# elif 'show' in user_text:
#     with open('solera.py', 'r') as file:
#         todos = file.readlines()
# else:
#     print("Command is not valid")
