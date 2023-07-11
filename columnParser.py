def correctSyntax(value):

    if type(value) == int:
        return str(value)

    strValue = str(value)
    arr = []
    element = ""

    for char in range(len(strValue)):
        try:
            number = int(strValue[char])
            element += str(number)

            if char == len(strValue) - 1:
                arr.append(element)

        except:
            if len(element) == 0:
                continue
            else:
                arr.append(element)
                element = ""

    return ",".join(arr)


def currencyToNumber(currency, currencyCode='R$'):
    """
    Função que transforma um valor formatado em moeda em número

    currency: Valor numérico
    currencyCode: Tipo de moeda
    Ex. currencyToNumber('R$ 120,00', 'R$') ----> '120.00'
    """
    try:
        currencyArray = currency.split(currencyCode)
        return currencyArray[-1].replace(".", "").replace(",", ".").strip()
    except:
        return currency


def changeColumnValue(df):
    vectorizeCorrectSyntax = np.vectorize(correctSyntax)
    vectorizeCurrencyToNumber = np.vectorize(currencyToNumber)

    listOfColumns = ['operation_id', 'order_id',
                     'air_ticket', 'cancellation_ticket']

    for (colname, colval) in df.items():
        if colname in listOfColumns:
            df[colname] = vectorizeCorrectSyntax(df[colname].values)

    listOfColumns = ['refund_value']

    for (colname, colval) in df.items():
        if colname in listOfColumns:
            df[colname] = vectorizeCurrencyToNumber(df[colname].values)

    return df
