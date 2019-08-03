find_package(Doxygen)

IF(NOT DOXYGEN_FOUND)
    MESSAGE(STATUS "No Doxygen found. Documentation won't be built")
ELSE()
    file(GLOB SOURCES ${CMAKE_CURRENT_LIST_DIR}/../mr/*)
    file(GLOB MARKDOWN_DOC ${CMAKE_CURRENT_LIST_DIR}/*.md)
    list(APPEND MARKDOWN_DOC ${CMAKE_CURRENT_LIST_DIR}/../README.md)

    CONFIGURE_FILE(Doxyfile.in Doxyfile @ONLY)
    CONFIGURE_FILE(header.html.in header.html @ONLY)
    CONFIGURE_FILE(doxygen.css html/doxygen.css COPYONLY)

    file(GLOB DOXYFILES ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile*)

    set(DOXYGEN_INPUT_DIR ${PROJECT_SOURCE_DIR})
    Message("Doxygen input dir ${DOXYGEN_INPUT_DIR}")
    add_custom_command(OUTPUT html
        COMMAND ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
        COMMAND ${CMAKE_COMMAND} -E touch ${CMAKE_CURRENT_BINARY_DIR}/html
        DEPENDS ${MARKDOWN_DOC} ${SOURCES} ${DOXYFILES}
        WORKING_DIRECTORY ${DOXYGEN_INPUT_DIR}
    )

    add_custom_target(doc ALL DEPENDS html)
    #install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/html
    #    DESTINATION ${DOC_INSTALL_DIR}
    #    COMPONENT doc)
ENDIF()
