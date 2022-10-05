from enum import Enum

class SilpoCategories(Enum):
    GROCERIES = 65
    FRUITS_AND_VEGETABLES = 374
    MEAT_FISH_BIRD = 277
    SAUSAGES = 316
    CHEESE = 1468
    BREAD = 486
    GASTRONOMY = 433
    MILK_AND_EGGS = 234
    FROZEN = 264
    PRESERVES_SAUSES_SPECIES = 130
    SWEETS = 298
    SNACKS = 308
    COFFEE_AND_TEA = 359
    DRINKS = 52
    ALCOHOL = 22
    CIGARETTES = 470
    HYGIENE = 535
    HOME = 567
    CHILDREN_THINGS = 449
    FOR_PETS = 653

class SiploBuckets:
    API_BUCKET = "silpo-api-data"